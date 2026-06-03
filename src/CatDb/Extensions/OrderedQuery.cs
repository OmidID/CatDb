// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections;
using System.Linq.Expressions;

namespace CatDb.Extensions;

/// <summary>
/// Ordered projection of a primary-key or secondary-index query.
///
/// <para>Produced by calling <c>.OrderBy(...)</c> / <c>.OrderByDescending(...)</c> on a
/// <see cref="TableQuery{TKey,TRecord}"/> or <see cref="IndexQuery{TKey,TRecord,TField}"/>.
/// The underlying query (its bounds, index, direction) decides <em>which</em> rows are
/// produced; this wrapper decides the order they come out in.</para>
///
/// <code>
/// table.Query(c => c.Email)
///      .AtLeast("user10@example.com")
///      .AtMost("user19@example.com")
///      .OrderBy(c => c.Name)            // primary sort key (ascending)
///      .OrderByDescending(c => c.Age)   // secondary sort key (descending)
/// </code>
///
/// <para><b>Sort target</b>: any record member (indexed or not) or the table key.
/// Sorting reads the value off the already-materialized record, so it works regardless
/// of whether a secondary index exists on the field.</para>
///
/// <para><b>Performance</b>: ordering is applied entirely on the filtered result set —
/// the WTree scan and the secondary-index lookup paths are untouched. The result set is
/// buffered once and sorted with a stable O(m log m) sort (m = rows after filtering), so
/// ties preserve the source order and the engine's hot paths take zero overhead.</para>
///
/// <para><b>Chaining</b>: each subsequent <c>OrderBy</c>/<c>OrderByDescending</c> appends a
/// lower-priority sort key (i.e. it behaves like LINQ's <c>ThenBy</c>). <c>ThenBy</c> /
/// <c>ThenByDescending</c> are provided as explicit aliases.</para>
///
/// Supports any field/key type — comparison uses <see cref="Comparer{T}.Default"/>.
/// </summary>
public sealed class OrderedQuery<TKey, TRecord> : IEnumerable<KeyValuePair<TKey, TRecord>>
{
    private readonly IEnumerable<KeyValuePair<TKey, TRecord>> _source;
    private readonly List<SortStep> _steps;
    private int? _take;
    private int _skip;

    internal OrderedQuery(IEnumerable<KeyValuePair<TKey, TRecord>> source, SortStep first)
    {
        _source = source;
        _steps = new List<SortStep> { first };
    }

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

    /// <summary>Add an ascending sort key on the primary key.</summary>
    public OrderedQuery<TKey, TRecord> OrderByKey()
        => Append(SortStep.ForKey(descending: false));

    /// <summary>Add a descending sort key on the primary key.</summary>
    public OrderedQuery<TKey, TRecord> OrderByKeyDescending()
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
        _steps.Add(step);
        return this;
    }

    // ── Execution ─────────────────────────────────────────────────────────────

    public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator()
    {
        // Buffer the filtered rows once. Pair each with its source position so the
        // sort is stable (equal keys keep their original relative order).
        var buffer = new List<KeyValuePair<TKey, TRecord>>(_source);
        var ordered = new int[buffer.Count];
        for (var i = 0; i < ordered.Length; i++)
            ordered[i] = i;

        var steps = _steps;
        Array.Sort(ordered, (a, b) =>
        {
            for (var s = 0; s < steps.Count; s++)
            {
                var c = steps[s].Compare(buffer[a], buffer[b]);
                if (c != 0) return c;
            }
            return a.CompareTo(b); // stable tie-break on original position
        });

        var skipped = 0;
        var produced = 0;
        foreach (var idx in ordered)
        {
            if (skipped < _skip) { skipped++; continue; }
            yield return buffer[idx];
            if (_take.HasValue && ++produced >= _take.Value)
                yield break;
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    // ── One comparison step (field or key, ascending or descending) ───────────

    /// <summary>
    /// A single type-erased sort key: compiles the selector once and compares two rows
    /// by that key, honoring the direction. Created via <see cref="ForField{TOrder}"/>
    /// or <see cref="ForKey"/>.
    /// </summary>
    internal sealed class SortStep
    {
        private readonly Comparison<KeyValuePair<TKey, TRecord>> _compare;

        private SortStep(Comparison<KeyValuePair<TKey, TRecord>> compare) => _compare = compare;

        public int Compare(KeyValuePair<TKey, TRecord> x, KeyValuePair<TKey, TRecord> y) => _compare(x, y);

        public static SortStep ForField<TOrder>(Expression<Func<TRecord, TOrder>> selector, bool descending)
        {
            ArgumentNullException.ThrowIfNull(selector);
            var get = selector.Compile();
            var comparer = Comparer<TOrder>.Default;
            Comparison<KeyValuePair<TKey, TRecord>> compare = descending
                ? (x, y) => comparer.Compare(get(y.Value), get(x.Value))
                : (x, y) => comparer.Compare(get(x.Value), get(y.Value));
            return new SortStep(compare);
        }

        public static SortStep ForKey(bool descending)
        {
            var comparer = Comparer<TKey>.Default;
            Comparison<KeyValuePair<TKey, TRecord>> compare = descending
                ? (x, y) => comparer.Compare(y.Key, x.Key)
                : (x, y) => comparer.Compare(x.Key, y.Key);
            return new SortStep(compare);
        }
    }
}
