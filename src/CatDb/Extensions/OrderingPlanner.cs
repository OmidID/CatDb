// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;
using CatDb.Database;

namespace CatDb.Extensions;

/// <summary>
/// Chooses how an <see cref="OrderedQuery{TKey,TRecord}"/> produces order.
///
/// <para>The high-value decision: if the leading ORDER BY field has its own single-field
/// secondary index, we can <b>drive iteration from that index</b> (which is a B-tree already
/// sorted by the field) and re-apply the original filter as a residual predicate — streaming
/// sorted output with bounded memory, instead of buffering and sorting the whole result set.</para>
/// </summary>
internal static class OrderingPlanner
{
    /// <summary>
    /// Returns a driver that streams the entire table in <paramref name="selector"/>'s field
    /// order (ascending or descending), or <c>null</c> if that field has no single-field index.
    /// </summary>
    public static Func<bool, IEnumerable<KeyValuePair<TKey, TRecord>>>? IndexDriver<TKey, TRecord, TOrder>(
        ITable<TKey, TRecord> table,
        Expression<Func<TRecord, TOrder>> selector)
    {
        if (selector.Body is not MemberExpression member)
            return null;

        var indexName = ResolveSingleFieldIndex(table, member.Member.Name);
        if (indexName is null)
            return null;

        // A fresh, criteria-free index query is a full ordered scan of that field's index.
        return descending => new IndexQuery<TKey, TRecord, TOrder>(table, indexName).StreamOrdered(descending);
    }

    private static string? ResolveSingleFieldIndex<TKey, TRecord>(ITable<TKey, TRecord> table, string memberName)
    {
        foreach (var idx in table.Indexes.ListIndexes())
            if (idx.MemberNames is { Length: 1 } names && names[0] == memberName)
                return idx.Name;
        return null;
    }
}
