// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections;
using System.Linq.Expressions;
using CatDb.Data;
using CatDb.Database;
using CatDb.Database.Querying;

namespace CatDb.Extensions;

/// <summary>
/// Fluent, field-oriented query builder — the single query surface. It accumulates a structured
/// <see cref="EngineQuery"/> (field predicates + ORDER BY keys + Skip/Take) and hands it to the
/// engine, which does <b>all</b> the work: index selection, multi-index AND intersection, residual
/// evaluation and ordering. This type carries no filtering/sorting logic of its own.
///
/// <code>
/// table.Query(q =&gt; q.Name).Equal("Omid")
///      .Then(q =&gt; q.Age).AtLeast(30).AtMost(50)
///      .Then(q =&gt; q.City).Equal("NYC")
///      .OrderBy(o =&gt; o.Age).ThenByDescending(o =&gt; o.Name)
///      .Take(20)
/// </code>
/// </summary>
public sealed class Query<TKey, TRecord> : IEnumerable<KeyValuePair<TKey, TRecord>>
{
    private readonly ITable<TKey, TRecord> _table;
    internal readonly EngineQuery Spec = new();

    private DataTransformer<TKey>? _keyTransform;
    private DataTransformer<TRecord>? _recordTransform;

    internal Query(ITable<TKey, TRecord> table) => _table = table;

    // ── Add another field predicate (AND) ─────────────────────────────────────

    /// <summary>Switch to another field to add more predicates (ANDed with the rest).</summary>
    public FieldCriteria<TKey, TRecord, TField> Then<TField>(Expression<Func<TRecord, TField>> selector)
        => new(this, MemberName(selector));

    // ── Ordering ──────────────────────────────────────────────────────────────
    // OrderBy* establishes the primary sort key and moves into the OrderedQuery stage, where only
    // ThenBy/ThenByDescending (lower-priority keys) + paging/terminal are available.

    public OrderedQuery<TKey, TRecord> OrderBy<TOrder>(Expression<Func<TRecord, TOrder>> selector)
        => Ordered(MemberName(selector), typeof(TOrder), descending: false);

    public OrderedQuery<TKey, TRecord> OrderByDescending<TOrder>(Expression<Func<TRecord, TOrder>> selector)
        => Ordered(MemberName(selector), typeof(TOrder), descending: true);

    /// <summary>Order by the primary key.</summary>
    public OrderedQuery<TKey, TRecord> OrderByKey() => Ordered(null, null, descending: false);

    public OrderedQuery<TKey, TRecord> OrderByKeyDescending() => Ordered(null, null, descending: true);

    private OrderedQuery<TKey, TRecord> Ordered(string? member, Type? fieldType, bool descending)
    {
        AddSort(member, fieldType, descending);
        return new OrderedQuery<TKey, TRecord>(this);
    }

    internal void AddSort(string? member, Type? fieldType, bool descending)
        => Spec.Sorts.Add(new SortField { Member = member, FieldType = fieldType, Descending = descending });

    // ── Primary-key range (engine key scan) ───────────────────────────────────

    public Query<TKey, TRecord> KeyEqual(TKey key)       => KeyFrom(key, true).KeyTo(key, true);
    public Query<TKey, TRecord> KeyAtLeast(TKey key)     => KeyFrom(key, true);
    public Query<TKey, TRecord> KeyGreaterThan(TKey key) => KeyFrom(key, false);
    public Query<TKey, TRecord> KeyAtMost(TKey key)      => KeyTo(key, true);
    public Query<TKey, TRecord> KeyLessThan(TKey key)    => KeyTo(key, false);

    public Query<TKey, TRecord> KeyBetween(TKey from, TKey to, bool fromInclusive = true, bool toInclusive = true)
        => KeyFrom(from, fromInclusive).KeyTo(to, toInclusive);

    private Query<TKey, TRecord> KeyFrom(TKey key, bool inclusive)
    {
        Spec.KeyFrom = new Data<TKey>(key); Spec.HasKeyFrom = true; Spec.KeyFromInclusive = inclusive;
        return this;
    }

    private Query<TKey, TRecord> KeyTo(TKey key, bool inclusive)
    {
        Spec.KeyTo = new Data<TKey>(key); Spec.HasKeyTo = true; Spec.KeyToInclusive = inclusive;
        return this;
    }

    // ── Paging ──────────────────────────────────────────────────────────────

    public Query<TKey, TRecord> Take(int count) { Spec.Take = count; return this; }
    public Query<TKey, TRecord> Skip(int count) { Spec.Skip = count; return this; }

    // ── Terminal ──────────────────────────────────────────────────────────────

    public long Count() => this.LongCount();
    public bool Any() { using var e = GetEnumerator(); return e.MoveNext(); }
    /// <summary>True if any record matches (alias of <see cref="Any"/>).</summary>
    public bool Exists() => Any();

    public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator()
    {
        foreach (var kv in _table.Indexes.ExecuteQuery(Spec))
            yield return Convert(kv);
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    // ── Internals ─────────────────────────────────────────────────────────────

    private KeyValuePair<TKey, TRecord> Convert(KeyValuePair<IData, IData> kv)
    {
        var key = kv.Key is Data<TKey> dk
            ? dk.Value
            : (_keyTransform ??= new DataTransformer<TKey>(_table.Descriptor.KeyType)).From(kv.Key);
        var record = kv.Value is Data<TRecord> dr
            ? dr.Value
            : (_recordTransform ??= new DataTransformer<TRecord>(_table.Descriptor.RecordType)).From(kv.Value);
        return new KeyValuePair<TKey, TRecord>(key, record);
    }

    internal static string MemberName<TField>(Expression<Func<TRecord, TField>> selector)
    {
        var body = selector.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } u)
            body = u.Operand;
        if (body is MemberExpression m)
            return m.Member.Name;
        throw new ArgumentException("Selector must be a simple member access, e.g. q => q.Age.", nameof(selector));
    }
}

/// <summary>
/// Criteria stage for one field of a <see cref="Query{TKey,TRecord}"/>. Each operator adds a
/// structured predicate and returns this stage, so several criteria can apply to the same field
/// (e.g. <c>.AtLeast(30).AtMost(50)</c>). Navigation methods (<c>Then</c>/<c>OrderBy</c>/<c>Take</c>/
/// enumeration) flow back to the parent query.
/// </summary>
public sealed class FieldCriteria<TKey, TRecord, TField> : IEnumerable<KeyValuePair<TKey, TRecord>>
{
    private readonly Query<TKey, TRecord> _query;
    private readonly string _member;

    internal FieldCriteria(Query<TKey, TRecord> query, string member)
    {
        _query = query;
        _member = member;
    }

    // ── Field predicates (each ANDs into the spec, returns this for more on same field) ──

    public FieldCriteria<TKey, TRecord, TField> Equal(TField value)        => Add(FilterOp.Equal, value);
    public FieldCriteria<TKey, TRecord, TField> AtLeast(TField value)      => Add(FilterOp.AtLeast, value);
    public FieldCriteria<TKey, TRecord, TField> GreaterThan(TField value)  => Add(FilterOp.GreaterThan, value);
    public FieldCriteria<TKey, TRecord, TField> AtMost(TField value)       => Add(FilterOp.AtMost, value);
    public FieldCriteria<TKey, TRecord, TField> LessThan(TField value)     => Add(FilterOp.LessThan, value);

    public FieldCriteria<TKey, TRecord, TField> Between(
        TField from, TField to, bool fromInclusive = true, bool toInclusive = true)
    {
        _query.Spec.Filters.Add(new FieldFilter
        {
            Member = _member, Op = FilterOp.Between, FieldType = typeof(TField),
            Value = new Data<TField>(from), Value2 = new Data<TField>(to),
            FromInclusive = fromInclusive, ToInclusive = toInclusive,
        });
        return this;
    }

    /// <summary>Prefix match (string fields): the field starts with <paramref name="value"/>.</summary>
    public FieldCriteria<TKey, TRecord, TField> StartsWith(TField value) => Add(FilterOp.Prefix, value);

    private FieldCriteria<TKey, TRecord, TField> Add(FilterOp op, TField value)
    {
        _query.Spec.Filters.Add(new FieldFilter
        {
            Member = _member, Op = op, FieldType = typeof(TField), Value = new Data<TField>(value),
        });
        return this;
    }

    // ── Navigation back to the query ────────────────────────────────────────────

    public FieldCriteria<TKey, TRecord, TField2> Then<TField2>(Expression<Func<TRecord, TField2>> selector)
        => _query.Then(selector);

    public OrderedQuery<TKey, TRecord> OrderBy<TOrder>(Expression<Func<TRecord, TOrder>> selector) => _query.OrderBy(selector);
    public OrderedQuery<TKey, TRecord> OrderByDescending<TOrder>(Expression<Func<TRecord, TOrder>> selector) => _query.OrderByDescending(selector);
    public OrderedQuery<TKey, TRecord> OrderByKey() => _query.OrderByKey();
    public OrderedQuery<TKey, TRecord> OrderByKeyDescending() => _query.OrderByKeyDescending();
    public Query<TKey, TRecord> Take(int count) => _query.Take(count);
    public Query<TKey, TRecord> Skip(int count) => _query.Skip(count);

    public Query<TKey, TRecord> KeyEqual(TKey key) => _query.KeyEqual(key);
    public Query<TKey, TRecord> KeyAtLeast(TKey key) => _query.KeyAtLeast(key);
    public Query<TKey, TRecord> KeyGreaterThan(TKey key) => _query.KeyGreaterThan(key);
    public Query<TKey, TRecord> KeyAtMost(TKey key) => _query.KeyAtMost(key);
    public Query<TKey, TRecord> KeyLessThan(TKey key) => _query.KeyLessThan(key);
    public Query<TKey, TRecord> KeyBetween(TKey from, TKey to, bool fromInclusive = true, bool toInclusive = true)
        => _query.KeyBetween(from, to, fromInclusive, toInclusive);

    public long Count() => _query.Count();
    public bool Any() => _query.Any();
    public bool Exists() => _query.Any();

    public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator() => _query.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>
/// Ordering stage of a <see cref="Query{TKey,TRecord}"/>, entered by <c>OrderBy</c>/<c>OrderByDescending</c>.
/// Exposes only lower-priority sort keys (<c>ThenBy</c>/<c>ThenByDescending</c>) plus paging and
/// terminals — filter predicates are no longer addable here (the filter is fixed once ordering begins).
/// </summary>
public sealed class OrderedQuery<TKey, TRecord> : IEnumerable<KeyValuePair<TKey, TRecord>>
{
    private readonly Query<TKey, TRecord> _query;

    internal OrderedQuery(Query<TKey, TRecord> query) => _query = query;

    /// <summary>Append a lower-priority ascending sort key (tie-breaker for the keys before it).</summary>
    public OrderedQuery<TKey, TRecord> ThenBy<TOrder>(Expression<Func<TRecord, TOrder>> selector)
    {
        _query.AddSort(Query<TKey, TRecord>.MemberName(selector), typeof(TOrder), descending: false);
        return this;
    }

    public OrderedQuery<TKey, TRecord> ThenByDescending<TOrder>(Expression<Func<TRecord, TOrder>> selector)
    {
        _query.AddSort(Query<TKey, TRecord>.MemberName(selector), typeof(TOrder), descending: true);
        return this;
    }

    /// <summary>Append the primary key as a lower-priority sort key.</summary>
    public OrderedQuery<TKey, TRecord> ThenByKey()
    {
        _query.AddSort(null, null, descending: false);
        return this;
    }

    public OrderedQuery<TKey, TRecord> ThenByKeyDescending()
    {
        _query.AddSort(null, null, descending: true);
        return this;
    }

    public OrderedQuery<TKey, TRecord> Take(int count) { _query.Take(count); return this; }
    public OrderedQuery<TKey, TRecord> Skip(int count) { _query.Skip(count); return this; }

    public long Count() => _query.Count();
    public bool Any() => _query.Any();
    public bool Exists() => _query.Any();

    public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator() => _query.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
