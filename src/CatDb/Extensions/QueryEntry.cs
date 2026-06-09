// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;
using CatDb.Database;

namespace CatDb.Extensions;

/// <summary>
/// Entry points for the fluent <see cref="Query{TKey,TRecord}"/> builder.
/// (Temporary name <c>Q</c> during the refactor; becomes <c>Query</c> once the old surface is removed.)
/// </summary>
public static class QueryEntryExtensions
{
    /// <summary>Begin a query with no field predicate (e.g. order all rows, or page by key).</summary>
    public static Query<TKey, TRecord> Query<TKey, TRecord>(this ITable<TKey, TRecord> table)
        => new(table);

    /// <summary>Begin a query on a record field: <c>table.Query(q =&gt; q.Name).Equal("Omid")…</c>.</summary>
    public static FieldCriteria<TKey, TRecord, TField> Query<TKey, TRecord, TField>(
        this ITable<TKey, TRecord> table, Expression<Func<TRecord, TField>> selector)
        => new Query<TKey, TRecord>(table).Then(selector);
}
