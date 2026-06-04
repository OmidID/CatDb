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
    private bool _backward;

    /// <summary>Compiled field accessor — present only when the query was built from a member
    /// selector (<c>Query(c =&gt; c.Field)</c>), enabling a residual filter for cross-index sorts.</summary>
    private readonly Func<TRecord, TField>? _fieldSelector;

    internal IndexQuery(ITable<TKey, TRecord> table, string indexName)
        : this(table, indexName, null) { }

    internal IndexQuery(ITable<TKey, TRecord> table, string indexName,
        Expression<Func<TRecord, TField>>? fieldSelector)
    {
        _table = table;
        _indexName = indexName;
        _fieldSelector = fieldSelector?.Compile();
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

    // ── Ordering (sort the filtered result set by any field or the key) ───────

    /// <summary>
    /// Order the results by a record field, ascending. Picks the cheapest strategy:
    /// <list type="bullet">
    /// <item>field == this query's indexed field → streaming index scan (no buffer);</item>
    /// <item>field has its own index → drive-from-sort-index (stream sorted, filter residually);</item>
    /// <item>otherwise → buffered stable sort.</item>
    /// </list>
    /// </summary>
    public OrderedQuery<TKey, TRecord> OrderBy<TOrder>(Expression<Func<TRecord, TOrder>> selector)
        => BuildOrder(selector, descending: false);

    /// <summary>Order the results by a record field, descending (same strategy selection as <see cref="OrderBy{TOrder}"/>).</summary>
    public OrderedQuery<TKey, TRecord> OrderByDescending<TOrder>(Expression<Func<TRecord, TOrder>> selector)
        => BuildOrder(selector, descending: true);

    private OrderedQuery<TKey, TRecord> BuildOrder<TOrder>(Expression<Func<TRecord, TOrder>> selector, bool descending)
    {
        // The residual replays this query's criteria — needed whenever ordering is driven from an
        // index other than this query's own criteria-applied scan (cross-field or covering composite).
        var residual = BuildResidual();
        // When this query filters by equality, a composite (filterField, sortKeys…) index can serve
        // the whole thing as one ordered range scan (resolved at enumeration).
        var prefix = BuildPrefixPlan();

        // 1) Same field as the index we are scanning → the source is already ordered by it.
        if (MatchesIndexField(selector))
        {
            Func<bool, IEnumerable<KeyValuePair<TKey, TRecord>>> sameFieldDriver = StreamOrdered;
            return new(_table, this, residual,
                OrderedQuery<TKey, TRecord>.SortStep.ForField(selector, descending, sameFieldDriver), prefix);
        }

        // 2) A different field that has its own single-field index → drive from it, filter residually.
        var crossDriver = OrderingPlanner.IndexDriver(_table, selector);
        if (crossDriver is not null)
            return new(_table, this, residual,
                OrderedQuery<TKey, TRecord>.SortStep.ForField(selector, descending, crossDriver), prefix);

        // 3) No single-field index on the sort key → buffered, unless a covering/prefix composite
        //    index (resolved at enumeration) can stream all keys.
        return new(_table, this, residual,
            OrderedQuery<TKey, TRecord>.SortStep.ForField(selector, descending), prefix);
    }

    /// <summary>
    /// If this query filters by equality on a single-field-indexed column, exposes a prefix scan so
    /// a composite (thatColumn, sortKeys…) index can serve the ORDER BY directly. Null otherwise.
    /// </summary>
    private OrderedQuery<TKey, TRecord>.PrefixPlan? BuildPrefixPlan()
    {
        if (!_hasEqual)
            return null;
        var def = _table.Indexes.GetIndex(_indexName);
        if (def is not { MemberNames.Length: 1 })
            return null;
        var equalData = new Data<TField>(_equalValue!);
        return new(def.MemberNames[0],
            (indexName, descending) =>
                _table.Indexes.FindByIndexPrefix(indexName, equalData, 1, descending).Select(ConvertPair));
    }

    /// <summary>
    /// True when the sort selector is a single member that equals this query's indexed field.
    /// </summary>
    private bool MatchesIndexField<TOrder>(Expression<Func<TRecord, TOrder>> selector)
    {
        if (selector.Body is not MemberExpression member)
            return false;
        var def = _table.Indexes.GetIndex(_indexName);
        return def is { MemberNames.Length: 1 } && def.MemberNames[0] == member.Member.Name;
    }

    /// <summary>
    /// Builds a residual predicate replaying this query's criteria on a materialized record —
    /// used when iteration is driven from a different index so non-matching rows are dropped.
    /// Returns null when the query has no selector (string-named index) or no criteria.
    /// </summary>
    private Func<KeyValuePair<TKey, TRecord>, bool>? BuildResidual()
    {
        if (_fieldSelector is null)
            return null;
        var sel = _fieldSelector;

        if (_hasEqual)
        {
            var value = _equalValue;
            var eq = System.Collections.Generic.EqualityComparer<TField>.Default;
            return kv => eq.Equals(sel(kv.Value), value!);
        }

        if (!_hasFrom && !_hasTo)
            return null; // full scan: no rows to drop

        var cmp = FieldComparer();
        TField? from = _from, to = _to;
        bool hasFrom = _hasFrom, hasTo = _hasTo, fromIncl = _fromInclusive, toIncl = _toInclusive;
        return kv =>
        {
            var f = sel(kv.Value);
            if (hasFrom)
            {
                var c = cmp.Compare(f, from!);
                if (c < 0 || (c == 0 && !fromIncl)) return false;
            }
            if (hasTo)
            {
                var c = cmp.Compare(f, to!);
                if (c > 0 || (c == 0 && !toIncl)) return false;
            }
            return true;
        };
    }

    /// <summary>Field comparer matching the engine's index ordering (DataComparer over the field type).</summary>
    private static IComparer<TField> FieldComparer()
    {
        var dc = new DataComparer(typeof(TField));
        return System.Collections.Generic.Comparer<TField>.Create(
            (a, b) => dc.Compare(new Data<TField>(a!), new Data<TField>(b!)));
    }

    /// <summary>Order the results by the primary key, ascending.</summary>
    public OrderedQuery<TKey, TRecord> OrderByKey()
        => new(_table, this, residual: null, OrderedQuery<TKey, TRecord>.SortStep.ForKey(descending: false));

    /// <summary>Order the results by the primary key, descending.</summary>
    public OrderedQuery<TKey, TRecord> OrderByKeyDescending()
        => new(_table, this, residual: null, OrderedQuery<TKey, TRecord>.SortStep.ForKey(descending: true));

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


    /// <summary>
    /// Scan in descending index-field order (engine backward scan, streamed).
    /// Equivalent to <c>OrderByDescending(theIndexedField)</c>.
    /// </summary>
    public IndexQuery<TKey, TRecord, TField> Backward()
    {
        _backward = true;
        return this;
    }

    // ── Execution (IEnumerable) ──────────────────────────────────────────────

    public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator()
        => Stream(_backward).GetEnumerator();

    /// <summary>
    /// Streams results from the secondary index in the given direction, applying <see cref="Take"/>.
    /// The engine reads the index in order (forward = ascending field, backward = descending field)
    /// with bounded memory; equality lookups ignore direction.
    /// </summary>
    private IEnumerable<KeyValuePair<TKey, TRecord>> Stream(bool backward)
    {
        var produced = 0;
        foreach (var kv in StreamOrdered(backward))
        {
            yield return kv;
            if (_take.HasValue && ++produced >= _take.Value)
                yield break;
        }
    }

    /// <summary>
    /// Streams the criteria-limited rows in index-field order (no <see cref="Take"/> cap) — used as
    /// an ordered driver by <see cref="OrderedQuery{TKey,TRecord}"/>.
    /// </summary>
    internal IEnumerable<KeyValuePair<TKey, TRecord>> StreamOrdered(bool backward)
    {
        IEnumerable<KeyValuePair<IData, IData>> source;

        if (_hasEqual)
        {
            var fieldData = new Data<TField>(_equalValue!);
            // Equality is a single field value; reverse the (possibly multi-key) order for descending.
            source = _table.Indexes.FindByIndex(_indexName, fieldData);
            if (backward) source = source.Reverse();
        }
        else
        {
            var fromData = _hasFrom ? (IData)new Data<TField>(_from!) : null;
            var toData = _hasTo ? (IData)new Data<TField>(_to!) : null;
            source = _table.Indexes.FindByIndexRange(
                _indexName, fromData, _hasFrom, _fromInclusive, toData, _hasTo, _toInclusive, backward);
        }

        foreach (var kv in source)
            yield return ConvertPair(kv);
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    // Portable-backed tables (e.g. remote) store keys/records as Data<Slots<…>>; typed tables store
    // Data<TKey>/Data<TRecord>. Cast the fast (typed) path; transform the portable one.
    private ITransformer<TKey, IData>? _keyTransform;
    private ITransformer<TRecord, IData>? _recordTransform;

    private KeyValuePair<TKey, TRecord> ConvertPair(KeyValuePair<IData, IData> kv)
    {
        var key = kv.Key is Data<TKey> dk
            ? dk.Value
            : (_keyTransform ??= new DataTransformer<TKey>(_table.Descriptor.KeyType)).From(kv.Key);
        var record = kv.Value is Data<TRecord> dr
            ? dr.Value
            : (_recordTransform ??= new DataTransformer<TRecord>(_table.Descriptor.RecordType)).From(kv.Value);
        return new KeyValuePair<TKey, TRecord>(key, record);
    }
}
