// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;

namespace CatDb.Extensions;

/// <summary>
/// Shared fluent surface for building a query's <c>WHERE</c> expression. Predicates are combined
/// left-to-right; <c>And</c>/<c>Or</c> set the next combinator and switch field. Every method returns
/// the same builder (<typeparamref name="TBuilder"/>) so the chain reads like the expression it builds.
/// </summary>
public interface IQueryBase<TKey, TRecord, TField, out TBuilder> : IEnumerable<KeyValuePair<TKey, TRecord>>
    where TBuilder : IQueryBase<TKey, TRecord, TField, TBuilder>
{
    TBuilder And<TSelect>(Expression<Func<TRecord, TSelect>> selector);
    TBuilder Or<TSelect>(Expression<Func<TRecord, TSelect>> selector);

    TBuilder Equal<TSelect>(TSelect value);
    TBuilder AtLeast<TSelect>(TSelect value);
    TBuilder GreaterThan<TSelect>(TSelect value);
    TBuilder AtMost<TSelect>(TSelect value);
    TBuilder LessThan<TSelect>(TSelect value);
    TBuilder Between<TSelect>(TSelect from, TSelect to, bool fromInclusive = true, bool toInclusive = true);
    TBuilder StartsWith<TSelect>(TSelect value);

    // Negated operators (engine NOT).
    TBuilder NotEqual<TSelect>(TSelect value);
    TBuilder NotAtLeast<TSelect>(TSelect value);
    TBuilder NotGreaterThan<TSelect>(TSelect value);
    TBuilder NotAtMost<TSelect>(TSelect value);
    TBuilder NotLessThan<TSelect>(TSelect value);
    TBuilder NotBetween<TSelect>(TSelect from, TSelect to, bool fromInclusive = true, bool toInclusive = true);
}

/// <summary>
/// Top-level query builder. Adds grouping (parentheses), primary-key range, ordering, paging and
/// terminals on top of <see cref="IQueryBase{TKey,TRecord,TField,TBuilder}"/>.
/// </summary>
public interface IQuery<TKey, TRecord, TField> : IQueryBase<TKey, TRecord, TField, IQuery<TKey, TRecord, TField>>
{
    /// <summary>Add a parenthesised sub-expression, combined with the pending AND/OR.</summary>
    IQuery<TKey, TRecord, TField> GroupOp(Action<IGroupOpQuery<TKey, TRecord, TField>> build);

    // Primary-key range.
    IQuery<TKey, TRecord, TField> KeyEqual(TKey key);
    IQuery<TKey, TRecord, TField> KeyAtLeast(TKey key);
    IQuery<TKey, TRecord, TField> KeyGreaterThan(TKey key);
    IQuery<TKey, TRecord, TField> KeyAtMost(TKey key);
    IQuery<TKey, TRecord, TField> KeyLessThan(TKey key);
    IQuery<TKey, TRecord, TField> KeyBetween(TKey from, TKey to, bool fromInclusive = true, bool toInclusive = true);

    // Ordering → ordered stage.
    IOrderedQuery<TKey, TRecord> OrderBy<TOrder>(Expression<Func<TRecord, TOrder>> selector);
    IOrderedQuery<TKey, TRecord> OrderByDescending<TOrder>(Expression<Func<TRecord, TOrder>> selector);
    IOrderedQuery<TKey, TRecord> OrderByKey();
    IOrderedQuery<TKey, TRecord> OrderByKeyDescending();

    // Paging + terminals.
    IQuery<TKey, TRecord, TField> Take(int count);
    IQuery<TKey, TRecord, TField> Skip(int count);
    long Count();
    bool Exists();
    /// <summary>Human-readable physical plan (EXPLAIN).</summary>
    string Explain();
}

/// <summary>
/// Builder for a parenthesised group. Same predicate surface plus parameterless <c>And()</c>/<c>Or()</c>
/// to set the combinator between members/sub-groups, and nested <see cref="GroupOp"/>.
/// </summary>
public interface IGroupOpQuery<TKey, TRecord, TField> : IQueryBase<TKey, TRecord, TField, IGroupOpQuery<TKey, TRecord, TField>>
{
    IGroupOpQuery<TKey, TRecord, TField> And();
    IGroupOpQuery<TKey, TRecord, TField> Or();
    IGroupOpQuery<TKey, TRecord, TField> GroupOp(Action<IGroupOpQuery<TKey, TRecord, TField>> build);
}

/// <summary>Ordering stage — only lower-priority sort keys plus paging/terminal.</summary>
public interface IOrderedQuery<TKey, TRecord> : IEnumerable<KeyValuePair<TKey, TRecord>>
{
    IOrderedQuery<TKey, TRecord> ThenBy<TOrder>(Expression<Func<TRecord, TOrder>> selector);
    IOrderedQuery<TKey, TRecord> ThenByDescending<TOrder>(Expression<Func<TRecord, TOrder>> selector);
    IOrderedQuery<TKey, TRecord> ThenByKey();
    IOrderedQuery<TKey, TRecord> ThenByKeyDescending();
    IOrderedQuery<TKey, TRecord> Take(int count);
    IOrderedQuery<TKey, TRecord> Skip(int count);
    long Count();
    bool Exists();
    string Explain();
}
