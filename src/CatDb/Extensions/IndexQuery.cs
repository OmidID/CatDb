// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections;
using System.Linq.Expressions;
using CatDb.Data;
using CatDb.Database;
using CatDb.Database.Indexing;

namespace CatDb.Extensions;

/// <summary>
/// Fluent query builder for secondary index queries.
/// Accumulates search criteria — executes lazily on enumeration.
///
/// <code>
/// table.Query(c => c.Email).Equals("test@test.com")
/// table.Query(c => c.Email).StartsWith("test")        // string fields
/// table.Query(c => c.City).Equals("NYC").Take(10)
/// table.Query(c => c.Price).Between(10.0, 50.0)
/// table.Query("CityAge").Equals(new { City = "NYC", Age = 30 })
/// table.Query(c => c.City).Equals("NYC").OrderBy(c => c.Name)
/// </code>
///
/// Supports any field type — not limited to primitives.
/// </summary>
public sealed class IndexQuery<TKey, TRecord, TField> : IEnumerable<KeyValuePair<TKey, TRecord>>
{
    private readonly ITable<TKey, TRecord> _table;
    private readonly string _indexName;
    private TField? _equalValue;
    private bool _hasEqual;
    private TField? _from;
    private bool _hasFrom;
    private bool _fromInclusive = true;
    private TField? _to;
    private bool _hasTo;
    private bool _toInclusive = true;
    private int? _take;
    private readonly List<Comparison<KeyValuePair<TKey, TRecord>>> _sortCriteria = new();

    internal IndexQuery(ITable<TKey, TRecord> table, string indexName)
    {
        _table = table;
        _indexName = indexName;
    }

    // ── Search criteria (return this for chaining) ───────────────────────────

    /// <summary>Find records where the indexed field equals <paramref name="value"/>.</summary>
    public IndexQuery<TKey, TRecord, TField> Equals(TField value)
    {
        _equalValue = value;
        _hasEqual = true;
        return this;
    }

    /// <summary>Lower bound inclusive: field &gt;= <paramref name="from"/>.</summary>
    public IndexQuery<TKey, TRecord, TField> AtLeast(TField from)
    {
        _from = from;
        _hasFrom = true;
        _fromInclusive = true;
        return this;
    }

    /// <summary>Lower bound exclusive: field &gt; <paramref name="from"/>.</summary>
    public IndexQuery<TKey, TRecord, TField> GreaterThan(TField from)
    {
        _from = from;
        _hasFrom = true;
        _fromInclusive = false;
        return this;
    }

    /// <summary>Upper bound inclusive: field &lt;= <paramref name="to"/>.</summary>
    public IndexQuery<TKey, TRecord, TField> AtMost(TField to)
    {
        _to = to;
        _hasTo = true;
        _toInclusive = true;
        return this;
    }

    /// <summary>Upper bound exclusive: field &lt; <paramref name="to"/>.</summary>
    public IndexQuery<TKey, TRecord, TField> LessThan(TField to)
    {
        _to = to;
        _hasTo = true;
        _toInclusive = false;
        return this;
    }

    /// <summary>Both bounds: field in [from, to].</summary>
    public IndexQuery<TKey, TRecord, TField> Between(TField from, TField to,
        bool fromInclusive = true, bool toInclusive = true)
    {
        _from = from;
        _hasFrom = true;
        _fromInclusive = fromInclusive;
        _to = to;
        _hasTo = true;
        _toInclusive = toInclusive;
        return this;
    }

    /// <summary>Limit the number of results returned.</summary>
    public IndexQuery<TKey, TRecord, TField> Take(int count)
    {
        _take = count;
        return this;
    }

    // ── Sorting ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Sort results ascending by the specified record field.
    /// Chaining multiple OrderBy calls applies secondary/tertiary sort keys.
    /// Sorting is applied lazily on enumeration — the filtered results are
    /// materialised, sorted, and then yielded.
    /// </summary>
    public IndexQuery<TKey, TRecord, TField> OrderBy<TSortField>(
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
    public IndexQuery<TKey, TRecord, TField> OrderByDescending<TSortField>(
        Expression<Func<TRecord, TSortField>> selector)
    {
        var compiled = selector.Compile();
        var comparer = System.Collections.Generic.Comparer<TSortField>.Default;
        _sortCriteria.Add((a, b) => comparer.Compare(compiled(b.Value), compiled(a.Value)));
        return this;
    }

    // ── String-specific: called via extension method ─────────────────────────

    internal IndexQuery<TKey, TRecord, TField> SetRange(
        TField? from, bool hasFrom, bool fromIncl,
        TField? to, bool hasTo, bool toIncl)
    {
        _from = from; _hasFrom = hasFrom; _fromInclusive = fromIncl;
        _to = to; _hasTo = hasTo; _toInclusive = toIncl;
        return this;
    }

    // ── Terminal operations ───────────────────────────────────────────────────

    /// <summary>Check if at least one record matches the search criteria.</summary>
    public bool Exists()
    {
        if (_hasEqual)
            return _table.Indexes.ExistsInIndex(_indexName, new Data<TField>(_equalValue!));

        // Range existence: check if any result comes back
        using var e = GetEnumerator();
        return e.MoveNext();
    }

    /// <summary>Count matching records without materializing all of them.</summary>
    public long Count()
    {
        if (_hasEqual)
            return _table.Indexes.CountByIndex(_indexName, new Data<TField>(_equalValue!));

        // Range count: must enumerate
        long n = 0;
        foreach (var _ in this) n++;
        return n;
    }


    // ── Execution (IEnumerable) ──────────────────────────────────────────────

    public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator()
    {
        IEnumerable<KeyValuePair<IData, IData>> source;

        if (_hasEqual)
        {
            var fieldData = new Data<TField>(_equalValue!);
            source = _table.Indexes.FindByIndex(_indexName, fieldData);
        }
        else if (_hasFrom || _hasTo)
        {
            var fromData = _hasFrom ? (IData)new Data<TField>(_from!) : null;
            var toData = _hasTo ? (IData)new Data<TField>(_to!) : null;
            source = _table.Indexes.FindByIndexRange(_indexName, fromData, _hasFrom, toData, _hasTo);
        }
        else
        {
            // No criteria — enumerate all index entries (full index scan)
            source = _table.Indexes.FindByIndexRange(_indexName, null, false, null, false);
        }

        if (_sortCriteria.Count > 0)
        {
            // Sorting requested: materialise, sort, then apply Take
            var list = new List<KeyValuePair<TKey, TRecord>>();
            foreach (var kv in source)
                list.Add(ConvertPair(kv));

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
            foreach (var kv in list)
            {
                yield return kv;
                if (_take.HasValue && ++produced >= _take.Value)
                    yield break;
            }
        }
        else
        {
            var produced = 0;
            foreach (var kv in source)
            {
                yield return ConvertPair(kv);
                if (_take.HasValue && ++produced >= _take.Value)
                    yield break;
            }
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private static KeyValuePair<TKey, TRecord> ConvertPair(KeyValuePair<IData, IData> kv)
    {
        var key = ((Data<TKey>)kv.Key).Value;
        var record = ((Data<TRecord>)kv.Value).Value;
        return new KeyValuePair<TKey, TRecord>(key, record);
    }
}
