// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections;
using System.Linq.Expressions;
using CatDb.Database;

namespace CatDb.Extensions;

/// <summary>
/// Fluent query builder for primary key range queries.
/// Accumulates bounds, direction, limits — executes lazily on enumeration.
///
/// <code>
/// table.Query().AtLeast(100).AtMost(200).Take(10)
/// table.Query().GreaterThan(lastKey).Take(20)          // cursor paging
/// table.Query().Backward().Take(5)                     // last 5
/// table.Query().StartsWith("abc")                      // string keys
/// table.Query().AtLeast(100).OrderBy(c => c.Name)      // sort by record field
/// </code>
///
/// Supports any key type — not limited to primitives.
/// </summary>
public sealed class TableQuery<TKey, TRecord> : IEnumerable<KeyValuePair<TKey, TRecord>>
{
    private readonly ITable<TKey, TRecord> _table;
    private TKey _from = default!;
    private bool _hasFrom;
    private bool _fromExclusive;
    private TKey _to = default!;
    private bool _hasTo;
    private bool _toExclusive;
    private int? _take;
    private int _skip;
    private bool _backward;
    private Func<TKey, bool>? _filter;
    private readonly List<Comparison<KeyValuePair<TKey, TRecord>>> _sortCriteria = new();

    internal TableQuery(ITable<TKey, TRecord> table) => _table = table;

    // ── Bound setters (return this for chaining) ─────────────────────────────

    /// <summary>Lower bound inclusive: key &gt;= <paramref name="from"/>.</summary>
    public TableQuery<TKey, TRecord> AtLeast(TKey from)
    {
        _from = from;
        _hasFrom = true;
        _fromExclusive = false;
        return this;
    }

    /// <summary>Lower bound exclusive: key &gt; <paramref name="from"/>.</summary>
    public TableQuery<TKey, TRecord> GreaterThan(TKey from)
    {
        _from = from;
        _hasFrom = true;
        _fromExclusive = true;
        return this;
    }

    /// <summary>Upper bound inclusive: key &lt;= <paramref name="to"/>.</summary>
    public TableQuery<TKey, TRecord> AtMost(TKey to)
    {
        _to = to;
        _hasTo = true;
        _toExclusive = false;
        return this;
    }

    /// <summary>Upper bound exclusive: key &lt; <paramref name="to"/>.</summary>
    public TableQuery<TKey, TRecord> LessThan(TKey to)
    {
        _to = to;
        _hasTo = true;
        _toExclusive = true;
        return this;
    }

    /// <summary>Both bounds: key in [from, to] (or open endpoints if exclusive).</summary>
    public TableQuery<TKey, TRecord> Between(TKey from, TKey to,
        bool fromInclusive = true, bool toInclusive = true)
    {
        _from = from;
        _hasFrom = true;
        _fromExclusive = !fromInclusive;
        _to = to;
        _hasTo = true;
        _toExclusive = !toInclusive;
        return this;
    }

    /// <summary>Limit the number of results returned.</summary>
    public TableQuery<TKey, TRecord> Take(int count)
    {
        _take = count;
        return this;
    }

    /// <summary>Skip the first N matching records (offset paging — prefer cursor paging for deep pages).</summary>
    public TableQuery<TKey, TRecord> Skip(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        _skip = count;
        return this;
    }

    /// <summary>Scan in descending key order.</summary>
    public TableQuery<TKey, TRecord> Backward()
    {
        _backward = true;
        return this;
    }

    /// <summary>Apply a post-scan key predicate.</summary>
    public TableQuery<TKey, TRecord> Where(Func<TKey, bool> predicate)
    {
        _filter = predicate;
        return this;
    }

    // ── Sorting ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Sort results ascending by the specified record field.
    /// Chaining multiple OrderBy calls applies secondary/tertiary sort keys.
    /// Sorting is applied lazily on enumeration — the scan results are
    /// materialised, sorted, and then yielded.
    /// </summary>
    public TableQuery<TKey, TRecord> OrderBy<TSortField>(
        Expression<Func<TRecord, TSortField>> selector)
    {
        var compiled = selector.Compile();
        var comparer = System.Collections.Generic.Comparer<TSortField>.Default;
        _sortCriteria.Add((a, b) => comparer.Compare(compiled(a.Value), compiled(b.Value)));
        return this;
    }

    /// <summary>
    /// Sort results descending by the specified record field.
    /// Chaining multiple OrderBy calls applies secondary/tertiary sort keys.
    /// </summary>
    public TableQuery<TKey, TRecord> OrderByDescending<TSortField>(
        Expression<Func<TRecord, TSortField>> selector)
    {
        var compiled = selector.Compile();
        var comparer = System.Collections.Generic.Comparer<TSortField>.Default;
        _sortCriteria.Add((a, b) => comparer.Compare(compiled(b.Value), compiled(a.Value)));
        return this;
    }

    // ── String-specific: called via extension method ─────────────────────────

    internal TableQuery<TKey, TRecord> SetBounds(TKey from, bool hasFrom, bool fromExcl,
                                                  TKey to,   bool hasTo,   bool toExcl)
    {
        _from = from; _hasFrom = hasFrom; _fromExclusive = fromExcl;
        _to = to; _hasTo = hasTo; _toExclusive = toExcl;
        return this;
    }

    // ── Terminal operations ───────────────────────────────────────────────────

    /// <summary>Count matching records without materializing.</summary>
    public long Count()
    {
        var q = BuildKeyQuery();
        return _table.ScanCount(q);
    }


    // ── Cursor paging helper ─────────────────────────────────────────────────

    /// <summary>
    /// Start after the given key (exclusive lower bound) — O(log N + take) cursor paging.
    /// </summary>
    public TableQuery<TKey, TRecord> After(TKey key) => GreaterThan(key);

    // ── Execution (IEnumerable) ──────────────────────────────────────────────

    public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator()
    {
        var q = BuildKeyQuery();
        IEnumerable<KeyValuePair<TKey, TRecord>> source;

        // When sorting is requested, we must scan the full range and materialise
        // before applying Skip/Take — disable optimised take paths.
        int takePlusSkip = _take.HasValue ? _take.Value + _skip : 0;
        bool useOptimizedTake = _take.HasValue && _sortCriteria.Count == 0;

        if (_backward)
        {
            if (useOptimizedTake && _table is XTable<TKey, TRecord> xt)
                source = xt.ScanBackwardTake(q, takePlusSkip);
            else if (useOptimizedTake && _table is XTablePortable<TKey, TRecord> xp)
                source = xp.ScanBackwardTake(q, takePlusSkip);
            else
                source = _table.ScanBackward(q);
        }
        else
        {
            if (useOptimizedTake && _table is XTable<TKey, TRecord> xt)
                source = xt.ScanTake(q, takePlusSkip);
            else if (useOptimizedTake && _table is XTablePortable<TKey, TRecord> xp)
                source = xp.ScanTake(q, takePlusSkip);
            else
                source = _table.Scan(q);
        }

        if (_sortCriteria.Count > 0)
        {
            // Sorting requested: materialise, sort, then apply Skip/Take
            var list = new List<KeyValuePair<TKey, TRecord>>();
            foreach (var kv in source)
                list.Add(kv);

            list.Sort((a, b) =>
            {
                foreach (var cmp in _sortCriteria)
                {
                    var result = cmp(a, b);
                    if (result != 0) return result;
                }
                return 0;
            });

            var produced = 0;
            for (int i = _skip; i < list.Count; i++)
            {
                yield return list[i];
                if (_take.HasValue && ++produced >= _take.Value)
                    yield break;
            }
        }
        else
        {
            var skipped = 0;
            var produced = 0;
            foreach (var kv in source)
            {
                if (skipped < _skip) { skipped++; continue; }
                yield return kv;
                if (_take.HasValue && ++produced >= _take.Value)
                    yield break;
            }
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private KeyQuery<TKey> BuildKeyQuery()
    {
        return new KeyQuery<TKey>(
            _from, _hasFrom, _fromExclusive,
            _to, _hasTo, _toExclusive,
            _filter);
    }
}
