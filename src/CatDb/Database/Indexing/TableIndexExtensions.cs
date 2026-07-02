// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;
using System.Reflection;
using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database.Indexing;

/// <summary>
/// Typed extension methods for <see cref="ITable{TKey, TRecord}"/>.
/// These provide a strongly-typed API for creating and querying secondary indexes
/// using expressions (e.g. <c>c => c.Email</c>) that get resolved to slot indices.
///
/// Works on ANY table instance: local, remote, typed, portable.
/// Does NOT modify <see cref="IStorageEngine"/> — attaches directly to the table.
/// </summary>
public static class TableIndexExtensions
{
    /// <summary>
    /// Creates a secondary index on a single property of the record type.
    /// </summary>
    /// <example>
    /// <code>
    /// var table = engine.OpenXTable&lt;int, Customer&gt;("customers");
    /// table.CreateIndex("Email", c => c.Email, IndexType.Unique);
    /// </code>
    /// </example>
    public static IndexDefinition CreateIndex<TKey, TRecord, TField>(
        this ITable<TKey, TRecord> table,
        string indexName,
        Expression<Func<TRecord, TField>> fieldSelector,
        IndexType type = IndexType.NonUnique)
    {
        var memberNames = ExtractMemberNames(fieldSelector);
        return table.Indexes.CreateIndex(indexName, memberNames, type);
    }

    /// <summary>
    /// Creates a composite index on multiple properties.
    /// </summary>
    /// <example>
    /// <code>
    /// table.CreateIndex("CityAge", new[] { "City", "Age" }, IndexType.NonUnique);
    /// </code>
    /// </example>
    public static IndexDefinition CreateIndex<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        string indexName,
        string[] memberNames,
        IndexType type = IndexType.NonUnique)
    {
        return table.Indexes.CreateIndex(indexName, memberNames, type);
    }

    /// <summary>
    /// Creates a composite index on multiple properties using slot indices.
    /// </summary>
    public static IndexDefinition CreateIndex<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        string indexName,
        int[] slotIndices,
        IndexType type = IndexType.NonUnique)
    {
        return table.Indexes.CreateIndex(indexName, slotIndices, type);
    }

    /// <summary>
    /// Finds all records whose indexed field equals the given value.
    /// </summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> FindByIndex<TKey, TRecord, TField>(
        this ITable<TKey, TRecord> table,
        string indexName,
        TField fieldValue)
    {
        IData fieldData = (object)fieldValue!;
        foreach (var kv in table.Indexes.FindByIndex(indexName, fieldData))
            yield return ConvertPair<TKey, TRecord>(kv);
    }

    /// <summary>
    /// Returns only the primary keys matching the indexed value.
    /// </summary>
    public static IEnumerable<TKey> FindKeysByIndex<TKey, TRecord, TField>(
        this ITable<TKey, TRecord> table,
        string indexName,
        TField fieldValue)
    {
        IData fieldData = (object)fieldValue!;
        foreach (var key in table.Indexes.FindKeysByIndex(indexName, fieldData))
            yield return (TKey)key;
    }

    /// <summary>
    /// Range search on an index.
    /// </summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> FindByIndexRange<TKey, TRecord, TField>(
        this ITable<TKey, TRecord> table,
        string indexName,
        TField from, bool hasFrom,
        TField to, bool hasTo,
        bool fromInclusive = true, bool toInclusive = true,
        bool backward = false)
    {
        var fromData = hasFrom ? (IData)(object)from! : null;
        var toData   = hasTo   ? (IData)(object)to!   : null;
        foreach (var kv in table.Indexes.FindByIndexRange(
            indexName, fromData, hasFrom, fromInclusive, toData, hasTo, toInclusive, backward))
            yield return ConvertPair<TKey, TRecord>(kv);
    }

    /// <summary>
    /// Checks if a value exists in the named index.
    /// </summary>
    public static bool ExistsInIndex<TKey, TRecord, TField>(
        this ITable<TKey, TRecord> table,
        string indexName,
        TField fieldValue)
    {
        return table.Indexes.ExistsInIndex(indexName, (object)fieldValue!);
    }

    /// <summary>
    /// Counts entries matching the given value in the named index.
    /// </summary>
    public static long CountByIndex<TKey, TRecord, TField>(
        this ITable<TKey, TRecord> table,
        string indexName,
        TField fieldValue)
    {
        return table.Indexes.CountByIndex(indexName, (object)fieldValue!);
    }

    /// <summary>Drops an index.</summary>
    public static void DropIndex<TKey, TRecord>(this ITable<TKey, TRecord> table, string indexName) =>
        table.Indexes.DropIndex(indexName);

    /// <summary>Rebuilds an index from scratch.</summary>
    public static void RebuildIndex<TKey, TRecord>(this ITable<TKey, TRecord> table, string indexName) =>
        table.Indexes.RebuildIndex(indexName);

    /// <summary>Rebuilds all indexes on the table.</summary>
    public static void RebuildAllIndexes<TKey, TRecord>(this ITable<TKey, TRecord> table) =>
        table.Indexes.RebuildAllIndexes();

    // ── Private helpers ──────────────────────────────────────────────────────

    private static string[] ExtractMemberNames<TRecord, TField>(Expression<Func<TRecord, TField>> selector)
    {
        var body = selector.Body;

        // Single member: c => c.Email
        if (body is MemberExpression member)
            return [member.Member.Name];

        // Composite (anonymous type): c => new { c.City, c.Age }
        if (body is NewExpression newExpr && newExpr.Arguments.Count > 0)
        {
            var names = new string[newExpr.Arguments.Count];
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                if (newExpr.Arguments[i] is MemberExpression m)
                    names[i] = m.Member.Name;
                else
                    throw new ArgumentException(
                        $"Composite index selector argument {i} must be a direct member access.");
            }
            return names;
        }

        throw new ArgumentException(
            "Index selector must be a member access (c => c.Email) or " +
            "anonymous type (c => new { c.City, c.Age }).");
    }

    private static KeyValuePair<TKey, TRecord> ConvertPair<TKey, TRecord>(KeyValuePair<IData, IData> kv)
        => new((TKey)kv.Key, (TRecord)kv.Value);
}
