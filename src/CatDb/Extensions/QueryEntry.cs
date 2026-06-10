// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;
using CatDb.Database;

namespace CatDb.Extensions;

/// <summary>Entry points for the fluent query builder (returns interfaces; impl is internal).</summary>
public static class QueryEntryExtensions
{
    /// <summary>Begin a query with no field selected (order all rows, page by key, or start a group).</summary>
    public static IQuery<TKey, TRecord, TKey> Query<TKey, TRecord>(this ITable<TKey, TRecord> table)
        => new Query<TKey, TRecord, TKey>(table);

    /// <summary>Begin a query on a record field: <c>table.Query(q =&gt; q.Name).Equal("Omid")…</c>.</summary>
    public static IQuery<TKey, TRecord, TField> Query<TKey, TRecord, TField>(
        this ITable<TKey, TRecord> table, Expression<Func<TRecord, TField>> selector)
        => new Query<TKey, TRecord, TField>(table, QueryMember.Name(selector), typeof(TField));

    /// <summary>Begin a top-level grouped expression.</summary>
    public static IGroupOpQuery<TKey, TRecord, TKey> GroupOp<TKey, TRecord>(this ITable<TKey, TRecord> table)
        => new Query<TKey, TRecord, TKey>(table);
}
