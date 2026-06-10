// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections;
using System.Linq.Expressions;
using CatDb.Data;
using CatDb.Database;
using CatDb.Database.Querying;

namespace CatDb.Extensions;

/// <summary>Extracts a simple member name from a selector (<c>q =&gt; q.Age</c>).</summary>
internal static class QueryMember
{
    public static string Name<TRecord, TField>(Expression<Func<TRecord, TField>> selector)
    {
        var body = selector.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } u) body = u.Operand;
        if (body is MemberExpression m) return m.Member.Name;
        throw new ArgumentException("Selector must be a simple member access, e.g. q => q.Age.", nameof(selector));
    }
}

/// <summary>Builds a boolean <see cref="FilterNode"/> tree left-to-right (AND default; Or flips next combine).</summary>
internal sealed class FilterAccumulator
{
    private FilterNode? _root;
    private bool _orPending;

    public void SetAnd() => _orPending = false;
    public void SetOr() => _orPending = true;

    public void Add(FieldFilter filter, bool negate)
    {
        FilterNode node = FilterNode.Leaf(filter);
        if (negate) node = FilterNode.Not(node);
        Combine(node);
    }

    public void Combine(FilterNode node)
    {
        _root = _root is null ? node : (_orPending ? FilterNode.Or(_root, node) : FilterNode.And(_root, node));
        _orPending = false;
    }

    public FilterNode? Build() => _root;
}

/// <summary>
/// The single internal query builder. Implements both the top-level <see cref="IQuery{TKey,TRecord,TField}"/>
/// and the grouping <see cref="IGroupOpQuery{TKey,TRecord,TField}"/> surfaces; the engine plans/executes
/// the assembled <see cref="EngineQuery"/>. All public access is through the interfaces.
/// </summary>
internal sealed class Query<TKey, TRecord, TField>
    : IQuery<TKey, TRecord, TField>, IGroupOpQuery<TKey, TRecord, TField>, IOrderedSource<TKey, TRecord>
{
    private readonly ITable<TKey, TRecord> _table;
    private readonly EngineQuery _spec = new();
    private readonly FilterAccumulator _filter = new();
    private string? _member;
    private Type? _memberType;   // the field's CLR type (from the selector) — values coerce to it

    private DataTransformer<TKey>? _keyTransform;
    private DataTransformer<TRecord>? _recordTransform;

    internal Query(ITable<TKey, TRecord> table, string? initialMember = null, Type? initialType = null)
    {
        _table = table;
        _member = initialMember;
        _memberType = initialType;
    }

    // ── Core mutators (shared by both interfaces) ─────────────────────────────

    private void DoAnd(string member, Type type) { _filter.SetAnd(); _member = member; _memberType = type; }
    private void DoOr(string member, Type type) { _filter.SetOr(); _member = member; _memberType = type; }

    private string Member()
        => _member ?? throw new InvalidOperationException("No field selected; call And/Or(selector) first.");

    private void DoLeaf<TSelect>(FilterOp op, TSelect value, bool negate)
    {
        var type = _memberType ?? typeof(TSelect);
        _filter.Add(new FieldFilter { Member = Member(), Op = op, FieldType = type, Value = MakeData(type, value) }, negate);
    }

    private void DoBetween<TSelect>(TSelect from, TSelect to, bool fromIncl, bool toIncl, bool negate)
    {
        var type = _memberType ?? typeof(TSelect);
        _filter.Add(new FieldFilter
        {
            Member = Member(), Op = FilterOp.Between, FieldType = type,
            Value = MakeData(type, from), Value2 = MakeData(type, to),
            FromInclusive = fromIncl, ToInclusive = toIncl,
        }, negate);
    }

    /// <summary>Wraps a value as <c>Data&lt;type&gt;</c>, coercing (e.g. int literal → double field).</summary>
    private static IData MakeData(Type type, object? value)
    {
        var target = Nullable.GetUnderlyingType(type) ?? type;
        if (value is not null && value.GetType() != target && target != typeof(object))
        {
            try { value = System.Convert.ChangeType(value, target, System.Globalization.CultureInfo.InvariantCulture); }
            catch { /* leave as-is; comparer will surface a clear error */ }
        }
        return (IData)Activator.CreateInstance(typeof(Data<>).MakeGenericType(type), value)!;
    }

    private void DoGroup(Action<IGroupOpQuery<TKey, TRecord, TField>> build)
    {
        var group = new Query<TKey, TRecord, TField>(_table);
        build(group);
        var node = group._filter.Build();
        if (node is not null) _filter.Combine(node);
    }

    // ── IQueryBase (explicit per interface — return types differ) ──────────────

    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.And<TSelect>(Expression<Func<TRecord, TSelect>> s) { DoAnd(QueryMember.Name(s), typeof(TSelect)); return this; }
    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.Or<TSelect>(Expression<Func<TRecord, TSelect>> s) { DoOr(QueryMember.Name(s), typeof(TSelect)); return this; }
    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.Equal<TSelect>(TSelect v) { DoLeaf(FilterOp.Equal, v, false); return this; }
    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.AtLeast<TSelect>(TSelect v) { DoLeaf(FilterOp.AtLeast, v, false); return this; }
    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.GreaterThan<TSelect>(TSelect v) { DoLeaf(FilterOp.GreaterThan, v, false); return this; }
    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.AtMost<TSelect>(TSelect v) { DoLeaf(FilterOp.AtMost, v, false); return this; }
    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.LessThan<TSelect>(TSelect v) { DoLeaf(FilterOp.LessThan, v, false); return this; }
    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.Between<TSelect>(TSelect f, TSelect t, bool fi, bool ti) { DoBetween(f, t, fi, ti, false); return this; }
    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.StartsWith<TSelect>(TSelect v) { DoLeaf(FilterOp.Prefix, v, false); return this; }
    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.NotEqual<TSelect>(TSelect v) { DoLeaf(FilterOp.Equal, v, true); return this; }
    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.NotAtLeast<TSelect>(TSelect v) { DoLeaf(FilterOp.AtLeast, v, true); return this; }
    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.NotGreaterThan<TSelect>(TSelect v) { DoLeaf(FilterOp.GreaterThan, v, true); return this; }
    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.NotAtMost<TSelect>(TSelect v) { DoLeaf(FilterOp.AtMost, v, true); return this; }
    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.NotLessThan<TSelect>(TSelect v) { DoLeaf(FilterOp.LessThan, v, true); return this; }
    IQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>.NotBetween<TSelect>(TSelect f, TSelect t, bool fi, bool ti) { DoBetween(f, t, fi, ti, true); return this; }

    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.And<TSelect>(Expression<Func<TRecord, TSelect>> s) { DoAnd(QueryMember.Name(s), typeof(TSelect)); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.Or<TSelect>(Expression<Func<TRecord, TSelect>> s) { DoOr(QueryMember.Name(s), typeof(TSelect)); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.Equal<TSelect>(TSelect v) { DoLeaf(FilterOp.Equal, v, false); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.AtLeast<TSelect>(TSelect v) { DoLeaf(FilterOp.AtLeast, v, false); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.GreaterThan<TSelect>(TSelect v) { DoLeaf(FilterOp.GreaterThan, v, false); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.AtMost<TSelect>(TSelect v) { DoLeaf(FilterOp.AtMost, v, false); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.LessThan<TSelect>(TSelect v) { DoLeaf(FilterOp.LessThan, v, false); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.Between<TSelect>(TSelect f, TSelect t, bool fi, bool ti) { DoBetween(f, t, fi, ti, false); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.StartsWith<TSelect>(TSelect v) { DoLeaf(FilterOp.Prefix, v, false); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.NotEqual<TSelect>(TSelect v) { DoLeaf(FilterOp.Equal, v, true); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.NotAtLeast<TSelect>(TSelect v) { DoLeaf(FilterOp.AtLeast, v, true); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.NotGreaterThan<TSelect>(TSelect v) { DoLeaf(FilterOp.GreaterThan, v, true); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.NotAtMost<TSelect>(TSelect v) { DoLeaf(FilterOp.AtMost, v, true); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.NotLessThan<TSelect>(TSelect v) { DoLeaf(FilterOp.LessThan, v, true); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>.NotBetween<TSelect>(TSelect f, TSelect t, bool fi, bool ti) { DoBetween(f, t, fi, ti, true); return this; }

    // ── IQuery (top-level extras) ──────────────────────────────────────────────

    public IQuery<TKey, TRecord, TField> GroupOp(Action<IGroupOpQuery<TKey, TRecord, TField>> build) { DoGroup(build); return this; }

    public IQuery<TKey, TRecord, TField> KeyEqual(TKey key) => KeyFrom(key, true).KeyTo(key, true);
    public IQuery<TKey, TRecord, TField> KeyAtLeast(TKey key) => KeyFrom(key, true);
    public IQuery<TKey, TRecord, TField> KeyGreaterThan(TKey key) => KeyFrom(key, false);
    public IQuery<TKey, TRecord, TField> KeyAtMost(TKey key) => KeyTo(key, true);
    public IQuery<TKey, TRecord, TField> KeyLessThan(TKey key) => KeyTo(key, false);
    public IQuery<TKey, TRecord, TField> KeyBetween(TKey from, TKey to, bool fromInclusive = true, bool toInclusive = true)
        => KeyFrom(from, fromInclusive).KeyTo(to, toInclusive);

    private Query<TKey, TRecord, TField> KeyFrom(TKey key, bool incl)
    { _spec.KeyFrom = new Data<TKey>(key); _spec.HasKeyFrom = true; _spec.KeyFromInclusive = incl; return this; }
    private Query<TKey, TRecord, TField> KeyTo(TKey key, bool incl)
    { _spec.KeyTo = new Data<TKey>(key); _spec.HasKeyTo = true; _spec.KeyToInclusive = incl; return this; }

    public IOrderedQuery<TKey, TRecord> OrderBy<TOrder>(Expression<Func<TRecord, TOrder>> s) => Ordered(QueryMember.Name(s), typeof(TOrder), false);
    public IOrderedQuery<TKey, TRecord> OrderByDescending<TOrder>(Expression<Func<TRecord, TOrder>> s) => Ordered(QueryMember.Name(s), typeof(TOrder), true);
    public IOrderedQuery<TKey, TRecord> OrderByKey() => Ordered(null, null, false);
    public IOrderedQuery<TKey, TRecord> OrderByKeyDescending() => Ordered(null, null, true);

    private IOrderedQuery<TKey, TRecord> Ordered(string? member, Type? type, bool desc)
    {
        AddSort(member, type, desc);
        return new OrderedQuery<TKey, TRecord>(this);
    }

    internal void AddSort(string? member, Type? type, bool desc)
        => _spec.Sorts.Add(new SortField { Member = member, FieldType = type, Descending = desc });

    public IQuery<TKey, TRecord, TField> Take(int count) { _spec.Take = count; return this; }
    public IQuery<TKey, TRecord, TField> Skip(int count) { _spec.Skip = count; return this; }

    public long Count() => this.LongCount();
    public bool Exists() { using var e = GetEnumerator(); return e.MoveNext(); }

    public string Explain()
    {
        _spec.Filter = _filter.Build();
        return _table.Indexes is CatDb.Database.Indexing.TableIndexManager m
            ? m.ExplainQuery(_spec)
            : "EXPLAIN unavailable (remote table).";
    }

    // ── IGroupOpQuery (parameterless combinators + group) ─────────────────────

    IGroupOpQuery<TKey, TRecord, TField> IGroupOpQuery<TKey, TRecord, TField>.And() { _filter.SetAnd(); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IGroupOpQuery<TKey, TRecord, TField>.Or() { _filter.SetOr(); return this; }
    IGroupOpQuery<TKey, TRecord, TField> IGroupOpQuery<TKey, TRecord, TField>.GroupOp(Action<IGroupOpQuery<TKey, TRecord, TField>> build) { DoGroup(build); return this; }

    // ── IOrderedSource (used by the ordering stage) ───────────────────────────

    void IOrderedSource<TKey, TRecord>.AddSort(string? member, Type? type, bool desc) => AddSort(member, type, desc);
    void IOrderedSource<TKey, TRecord>.SetTake(int count) => _spec.Take = count;
    void IOrderedSource<TKey, TRecord>.SetSkip(int count) => _spec.Skip = count;

    // ── Execution ──────────────────────────────────────────────────────────────

    internal EngineQuery Spec { get { _spec.Filter = _filter.Build(); return _spec; } }

    public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator()
    {
        _spec.Filter = _filter.Build();
        foreach (var kv in _table.Indexes.ExecuteQuery(_spec))
            yield return Convert(kv);
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

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
}

/// <summary>Non-generic-over-field hook the ordering stage uses to drive its parent query.</summary>
internal interface IOrderedSource<TKey, TRecord> : IEnumerable<KeyValuePair<TKey, TRecord>>
{
    void AddSort(string? member, Type? type, bool desc);
    void SetTake(int count);
    void SetSkip(int count);
    long Count();
    bool Exists();
    string Explain();
}

/// <summary>Internal ordering stage delegating to its parent query.</summary>
internal sealed class OrderedQuery<TKey, TRecord>(IOrderedSource<TKey, TRecord> source) : IOrderedQuery<TKey, TRecord>
{
    public IOrderedQuery<TKey, TRecord> ThenBy<TOrder>(Expression<Func<TRecord, TOrder>> s) { source.AddSort(QueryMember.Name(s), typeof(TOrder), false); return this; }
    public IOrderedQuery<TKey, TRecord> ThenByDescending<TOrder>(Expression<Func<TRecord, TOrder>> s) { source.AddSort(QueryMember.Name(s), typeof(TOrder), true); return this; }
    public IOrderedQuery<TKey, TRecord> ThenByKey() { source.AddSort(null, null, false); return this; }
    public IOrderedQuery<TKey, TRecord> ThenByKeyDescending() { source.AddSort(null, null, true); return this; }
    public IOrderedQuery<TKey, TRecord> Take(int count) { source.SetTake(count); return this; }
    public IOrderedQuery<TKey, TRecord> Skip(int count) { source.SetSkip(count); return this; }
    public long Count() => source.Count();
    public bool Exists() => source.Exists();
    public string Explain() => source.Explain();

    public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator() => source.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
