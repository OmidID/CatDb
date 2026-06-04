// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections;
using System.Linq.Expressions;
using CatDb.Data;
using CatDb.Database;

namespace CatDb.Extensions;

/// <summary>
/// Ordered projection of a primary-key or secondary-index query.
///
/// <para>Produced by <c>.OrderBy(...)</c> / <c>.OrderByDescending(...)</c> on a
/// <see cref="TableQuery{TKey,TRecord}"/> or <see cref="IndexQuery{TKey,TRecord,TField}"/>.</para>
///
/// <para><b>Three execution strategies</b>, chosen automatically:</para>
/// <list type="number">
/// <item><b>Pre-ordered stream</b> — the engine already emits rows in the requested order
/// (primary-key <c>OrderByKey</c>, or an index query ordered by its own field). Zero buffering.</item>
/// <item><b>Drive-from-sort-index</b> — the leading ORDER BY field has its own index, but it is
/// not the field that was filtered. Iteration is driven from that index (sorted) and the original
/// filter is re-applied as a residual predicate. Streams sorted; only equal-leading-key <em>runs</em>
/// are buffered when there are secondary sort keys. Memory is bounded by the largest run.</item>
/// <item><b>Buffered stable sort</b> — leading key not indexed: materialize the filtered result set
/// and stable-sort it. Correct for any combination, bounded by RAM.</item>
/// </list>
///
/// <para>Chaining appends lower-priority keys (ThenBy semantics). <c>Take</c>/<c>Skip</c> apply after
/// ordering. Comparison uses <see cref="Comparer{T}.Default"/> for presentation order.</para>
/// </summary>
public sealed class OrderedQuery<TKey, TRecord> : IEnumerable<KeyValuePair<TKey, TRecord>>
{
    private readonly IEnumerable<KeyValuePair<TKey, TRecord>> _filtered;
    private readonly Func<KeyValuePair<TKey, TRecord>, bool>? _residual;
    private readonly ITable<TKey, TRecord>? _table; // for covering-composite-index lookup
    private readonly PrefixPlan? _prefix;           // equality-filter prefix on a composite index

    /// <summary>Sort keys for the drive/buffer paths, or <c>null</c> for a pre-ordered stream.</summary>
    private readonly List<SortStep>? _steps;
    private readonly IEnumerable<KeyValuePair<TKey, TRecord>>? _preordered;
    private int? _take;
    private int _skip;

    internal OrderedQuery(
        ITable<TKey, TRecord> table,
        IEnumerable<KeyValuePair<TKey, TRecord>> filtered,
        Func<KeyValuePair<TKey, TRecord>, bool>? residual,
        SortStep first,
        PrefixPlan? prefix = null)
    {
        _table = table;
        _filtered = filtered;
        _residual = residual;
        _prefix = prefix;
        _steps = new List<SortStep> { first };
    }

    /// <summary>
    /// Describes an equality filter on the leading column of a composite index: scanning that
    /// composite with the prefix bound yields rows already ordered by the trailing column(s),
    /// turning <c>WHERE a = v ORDER BY b</c> into one index range scan (no residual, no heap fetch).
    /// </summary>
    internal sealed record PrefixPlan(
        string FilterMember,
        Func<string, bool, IEnumerable<KeyValuePair<TKey, TRecord>>> Scan);

    private OrderedQuery(IEnumerable<KeyValuePair<TKey, TRecord>> preordered)
    {
        _filtered = preordered;
        _preordered = preordered;
        _steps = null;
    }

    /// <summary>Wraps a source already in final order (engine Forward/Backward). Streams, no buffer.</summary>
    internal static OrderedQuery<TKey, TRecord> Streaming(IEnumerable<KeyValuePair<TKey, TRecord>> orderedSource)
        => new(orderedSource);

    // ── Additional sort keys (appended as lower-priority / ThenBy) ─────────────

    /// <summary>Add an ascending sort key on a record field.</summary>
    public OrderedQuery<TKey, TRecord> OrderBy<TOrder>(Expression<Func<TRecord, TOrder>> selector)
        => Append(SortStep.ForField(selector, descending: false));

    /// <summary>Add a descending sort key on a record field.</summary>
    public OrderedQuery<TKey, TRecord> OrderByDescending<TOrder>(Expression<Func<TRecord, TOrder>> selector)
        => Append(SortStep.ForField(selector, descending: true));

    /// <summary>Add an ascending sort key on a record field (alias of <see cref="OrderBy{TOrder}"/>).</summary>
    public OrderedQuery<TKey, TRecord> ThenBy<TOrder>(Expression<Func<TRecord, TOrder>> selector)
        => Append(SortStep.ForField(selector, descending: false));

    /// <summary>Add a descending sort key on a record field (alias of <see cref="OrderByDescending{TOrder}"/>).</summary>
    public OrderedQuery<TKey, TRecord> ThenByDescending<TOrder>(Expression<Func<TRecord, TOrder>> selector)
        => Append(SortStep.ForField(selector, descending: true));

    /// <summary>Add the primary key as a lower-priority (tie-break) ascending sort key.</summary>
    public OrderedQuery<TKey, TRecord> ThenByKey()
        => Append(SortStep.ForKey(descending: false));

    /// <summary>Add the primary key as a lower-priority (tie-break) descending sort key.</summary>
    public OrderedQuery<TKey, TRecord> ThenByKeyDescending()
        => Append(SortStep.ForKey(descending: true));

    // ── Result shaping (applied after ordering) ───────────────────────────────

    /// <summary>Limit the number of ordered results returned.</summary>
    public OrderedQuery<TKey, TRecord> Take(int count)
    {
        _take = count;
        return this;
    }

    /// <summary>Skip the first N ordered results (offset paging).</summary>
    public OrderedQuery<TKey, TRecord> Skip(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        _skip = count;
        return this;
    }

    private OrderedQuery<TKey, TRecord> Append(SortStep step)
    {
        if (_steps is null)
            throw new InvalidOperationException(
                "Cannot add a sort key after OrderByKey/OrderByKeyDescending: the primary key " +
                "is a total order, so secondary keys are redundant. Order by a field first, " +
                "then ThenByKey if you need the key as a tie-break.");
        _steps.Add(step);
        return this;
    }

    // ── Execution ─────────────────────────────────────────────────────────────

    public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator()
    {
        IEnumerable<KeyValuePair<TKey, TRecord>> ordered;
        if (_preordered is not null)
            ordered = _preordered;
        else
            // Plan precedence (cheapest first): equality-prefix on a composite index → covering
            // composite → single-leading index drive (run-sort the rest) → buffered stable sort.
            ordered = TryPrefixDrive()
                   ?? TryCompositeDrive()
                   ?? (_steps![0].Driver is not null ? DriveOrdered() : BufferOrdered());

        return Shape(ordered).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>
    /// If a single secondary index covers all the sort keys in order, drive it: the composite
    /// B-tree is already ordered by (k1, k2, …). When every key shares the lead direction the scan
    /// IS the final order (O(1) memory). With <b>mixed directions</b> we still drive the composite
    /// (exact k1 grouping) and only run-sort each equal-k1 group by the remaining target directions.
    /// Returns null when no covering index exists or any key is not a record member.
    /// </summary>
    /// <summary>
    /// When the query has an equality filter on column A and a composite index (A, sortKeys…) exists
    /// with all sort keys in one direction, scan that composite restricted to A = v: the rows come
    /// out already ordered by the sort keys — one index range scan, no residual, no heap fetch.
    /// </summary>
    private IEnumerable<KeyValuePair<TKey, TRecord>>? TryPrefixDrive()
    {
        if (_prefix is null || _table is null)
            return null;

        var names = new string[_steps!.Count + 1];
        names[0] = _prefix.FilterMember;
        var allSameDirection = true;
        for (var i = 0; i < _steps.Count; i++)
        {
            if (_steps[i].MemberName is not { } name)
                return null;
            names[i + 1] = name;
            if (_steps[i].Descending != _steps[0].Descending)
                allSameDirection = false;
        }

        var indexName = ResolveCoveringIndex(names);
        if (indexName is null)
            return null;

        // The prefix bound IS the filter, so no residual. All-same direction → the scan is the final
        // order; mixed → run-sort each equal-leading group (peak memory = largest group).
        var lead = _steps[0];
        var src = _prefix.Scan(indexName, lead.Descending);
        return allSameDirection ? src : RunSorted(src, lead);
    }

    private IEnumerable<KeyValuePair<TKey, TRecord>>? TryCompositeDrive()
    {
        if (_table is null || _steps!.Count < 2)
            return null;

        var names = new string[_steps.Count];
        var allSameDirection = true;
        for (var i = 0; i < _steps.Count; i++)
        {
            if (_steps[i].MemberName is not { } name)
                return null; // key-based step can't be in a record-member composite
            names[i] = name;
            if (_steps[i].Descending != _steps[0].Descending)
                allSameDirection = false;
        }

        var indexName = ResolveCoveringIndex(names);
        if (indexName is null)
            return null;

        var lead = _steps[0];
        var src = CompositeScan(indexName, lead.Descending);
        if (_residual is not null)
            src = Filter(src, _residual);

        // All-same direction → composite scan is the final order. Mixed → run-sort the groups.
        return allSameDirection ? src : RunSorted(src, lead);
    }

    private string? ResolveCoveringIndex(string[] orderByMembers)
    {
        foreach (var idx in _table!.Indexes.ListIndexes())
            if (idx.MemberNames is { } members &&
                members.Length == orderByMembers.Length &&
                members.AsSpan().SequenceEqual(orderByMembers))
                return idx.Name;
        return null;
    }

    /// <summary>Full ordered scan of a (composite) index, converted to typed pairs.</summary>
    private IEnumerable<KeyValuePair<TKey, TRecord>> CompositeScan(string indexName, bool descending)
    {
        foreach (var kv in _table!.Indexes.FindByIndexRange(
                     indexName, null, false, true, null, false, true, descending))
            yield return new KeyValuePair<TKey, TRecord>(
                ((Data<TKey>)kv.Key).Value, ((Data<TRecord>)kv.Value).Value);
    }

    /// <summary>Applies Skip/Take after the order is established (streaming).</summary>
    private IEnumerable<KeyValuePair<TKey, TRecord>> Shape(IEnumerable<KeyValuePair<TKey, TRecord>> ordered)
    {
        var skipped = 0;
        var produced = 0;
        foreach (var kv in ordered)
        {
            if (skipped < _skip) { skipped++; continue; }
            yield return kv;
            if (_take.HasValue && ++produced >= _take.Value)
                yield break;
        }
    }

    /// <summary>
    /// Drive-from-sort-index: stream from the leading key's index (already sorted), re-apply the
    /// residual filter, and run-sort within equal-leading-key groups when secondary keys exist.
    /// </summary>
    private IEnumerable<KeyValuePair<TKey, TRecord>> DriveOrdered()
    {
        var lead = _steps![0];
        var src = lead.Driver!(lead.Descending);
        if (_residual is not null)
            src = Filter(src, _residual);

        if (_steps.Count == 1)
            return src; // single key: the index order is the final order

        return RunSorted(src, lead);
    }

    private static IEnumerable<KeyValuePair<TKey, TRecord>> Filter(
        IEnumerable<KeyValuePair<TKey, TRecord>> src, Func<KeyValuePair<TKey, TRecord>, bool> residual)
    {
        foreach (var kv in src)
            if (residual(kv))
                yield return kv;
    }

    /// <summary>
    /// Streams the leading-key-ordered source, buffering only a run of rows sharing the same
    /// leading key and stable-sorting each run by the remaining keys. Peak memory = largest run.
    /// </summary>
    private IEnumerable<KeyValuePair<TKey, TRecord>> RunSorted(
        IEnumerable<KeyValuePair<TKey, TRecord>> src, SortStep lead)
    {
        var run = new List<KeyValuePair<TKey, TRecord>>();
        var hasPrev = false;
        KeyValuePair<TKey, TRecord> prev = default;

        foreach (var kv in src)
        {
            if (hasPrev && lead.Compare(prev, kv) != 0)
            {
                foreach (var x in SortRun(run)) yield return x;
                run.Clear();
            }
            run.Add(kv);
            prev = kv;
            hasPrev = true;
        }
        if (run.Count > 0)
            foreach (var x in SortRun(run)) yield return x;
    }

    /// <summary>Stable-sorts one equal-leading-key run by the secondary sort keys (steps[1..]).</summary>
    private IEnumerable<KeyValuePair<TKey, TRecord>> SortRun(List<KeyValuePair<TKey, TRecord>> run)
    {
        if (run.Count == 1) { yield return run[0]; yield break; }

        var steps = _steps!;
        Comparison<int> compare = (a, b) =>
        {
            for (var s = 1; s < steps.Count; s++)
            {
                var c = steps[s].Compare(run[a], run[b]);
                if (c != 0) return c;
            }
            return a.CompareTo(b); // stable
        };

        // Only the first (Skip+Take) rows of the whole output are ever needed, so a run never has to
        // retain more than that — bounds peak memory for low-cardinality leading keys.
        foreach (var i in OrderIndices(run.Count, compare, OutputLimit()))
            yield return run[i];
    }

    /// <summary>
    /// Buffered path: materialize the filtered rows and order them. With a Take, only the top
    /// (Skip+Take) are retained via a bounded heap (O(N log k) time, O(k) memory) — no full sort.
    /// </summary>
    private IEnumerable<KeyValuePair<TKey, TRecord>> BufferOrdered()
    {
        var buffer = new List<KeyValuePair<TKey, TRecord>>(_filtered);
        var steps = _steps!;
        Comparison<int> compare = (a, b) =>
        {
            for (var s = 0; s < steps.Count; s++)
            {
                var c = steps[s].Compare(buffer[a], buffer[b]);
                if (c != 0) return c;
            }
            return a.CompareTo(b); // stable tie-break on original position
        };

        foreach (var i in OrderIndices(buffer.Count, compare, OutputLimit()))
            yield return buffer[i];
    }

    /// <summary>Total rows the consumer can take (Skip+Take), or null for unbounded.</summary>
    private int? OutputLimit() => _take.HasValue ? checked(_skip + _take.Value) : null;

    /// <summary>
    /// Returns row indices [0, count) ordered by <paramref name="compare"/> (stable). When
    /// <paramref name="limit"/> is set and smaller than <paramref name="count"/>, only the smallest
    /// <paramref name="limit"/> are computed via a bounded max-heap — Top-K, O(count·log limit) time
    /// and O(limit) memory instead of a full sort.
    /// </summary>
    private static int[] OrderIndices(int count, Comparison<int> compare, int? limit)
    {
        if (limit is not { } k || k >= count)
        {
            var all = new int[count];
            for (var i = 0; i < count; i++) all[i] = i;
            Array.Sort(all, compare);
            return all;
        }
        if (k <= 0)
            return [];

        // Max-heap of size k keyed by `compare`: the root is the worst row currently kept, so a new
        // row that is better than the root evicts it.
        var heap = new int[k];
        var n = 0;
        for (var i = 0; i < count; i++)
        {
            if (n < k)
            {
                heap[n] = i;
                SiftUp(heap, n, compare);
                n++;
            }
            else if (compare(i, heap[0]) < 0)
            {
                heap[0] = i;
                SiftDown(heap, n, compare);
            }
        }
        Array.Sort(heap, 0, n, System.Collections.Generic.Comparer<int>.Create(compare));
        return heap;
    }

    private static void SiftUp(int[] heap, int i, Comparison<int> compare)
    {
        while (i > 0)
        {
            var parent = (i - 1) >> 1;
            if (compare(heap[i], heap[parent]) <= 0) break;
            (heap[i], heap[parent]) = (heap[parent], heap[i]);
            i = parent;
        }
    }

    private static void SiftDown(int[] heap, int n, Comparison<int> compare)
    {
        var i = 0;
        while (true)
        {
            var left = 2 * i + 1;
            if (left >= n) break;
            var largest = left;
            var right = left + 1;
            if (right < n && compare(heap[right], heap[left]) > 0) largest = right;
            if (compare(heap[largest], heap[i]) <= 0) break;
            (heap[i], heap[largest]) = (heap[largest], heap[i]);
            i = largest;
        }
    }

    // ── One comparison step (field or key, optional index driver) ─────────────

    /// <summary>
    /// A single type-erased sort key: a compiled comparison, a direction, and (for a field that
    /// has its own index) an optional ordered <see cref="Driver"/> used by the drive path.
    /// </summary>
    internal sealed class SortStep
    {
        private readonly Comparison<KeyValuePair<TKey, TRecord>> _compare;

        /// <summary>Descending direction for the drive path.</summary>
        public bool Descending { get; }

        /// <summary>Ordered index driver (ascending/descending) for this key, or null.</summary>
        public Func<bool, IEnumerable<KeyValuePair<TKey, TRecord>>>? Driver { get; }

        /// <summary>Record member name this key sorts on, or null (key-based / non-member).</summary>
        public string? MemberName { get; }

        private SortStep(Comparison<KeyValuePair<TKey, TRecord>> compare, bool descending,
            Func<bool, IEnumerable<KeyValuePair<TKey, TRecord>>>? driver, string? memberName)
        {
            _compare = compare;
            Descending = descending;
            Driver = driver;
            MemberName = memberName;
        }

        public int Compare(KeyValuePair<TKey, TRecord> x, KeyValuePair<TKey, TRecord> y) => _compare(x, y);

        public static SortStep ForField<TOrder>(
            Expression<Func<TRecord, TOrder>> selector,
            bool descending,
            Func<bool, IEnumerable<KeyValuePair<TKey, TRecord>>>? driver = null)
        {
            ArgumentNullException.ThrowIfNull(selector);
            var get = selector.Compile();
            var memberName = (selector.Body as MemberExpression)?.Member.Name;
            var comparer = System.Collections.Generic.Comparer<TOrder>.Default;
            Comparison<KeyValuePair<TKey, TRecord>> compare = descending
                ? (x, y) => comparer.Compare(get(y.Value), get(x.Value))
                : (x, y) => comparer.Compare(get(x.Value), get(y.Value));
            return new SortStep(compare, descending, driver, memberName);
        }

        public static SortStep ForKey(bool descending)
        {
            var comparer = System.Collections.Generic.Comparer<TKey>.Default;
            Comparison<KeyValuePair<TKey, TRecord>> compare = descending
                ? (x, y) => comparer.Compare(y.Key, x.Key)
                : (x, y) => comparer.Compare(x.Key, y.Key);
            return new SortStep(compare, descending, driver: null, memberName: null);
        }
    }
}
