// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;
using CatDb.Database;
using CatDb.Database.Indexing;

namespace CatDb.Extensions;

/// <summary>
/// Fluent query API entry points for <see cref="ITable{TKey,TRecord}"/>.
///
/// <para><b>Primary key queries</b> (B-tree ordered scan):</para>
/// <code>
/// table.Query().AtLeast(100).AtMost(200).Take(10)
/// table.Query().GreaterThan(lastKey).Take(20)      // cursor paging
/// table.Query().Backward().Take(5)                 // last 5
/// table.Query().StartsWith("abc")                  // string keys
/// table.Query().Count()                            // total records in range
/// </code>
///
/// <para><b>Index queries</b> (secondary index lookup):</para>
/// <code>
/// table.Query(c => c.Email).Equals("test@test.com")
/// table.Query(c => c.Email).StartsWith("sexy")
/// table.Query(c => c.City).Equals("NYC").Take(10)
/// table.Query(c => c.Price).Between(10.0, 50.0)
/// table.Query(c => c.Price).AtLeast(100.0).Take(5)
/// table.Query("MyIndex").Equals(someValue)         // explicit index name
/// </code>
///
/// <para><b>Execution</b>: queries are lazy — no work happens until you enumerate
/// (foreach, ToList, FirstOrDefault) or call a terminal (.Count(), .Exists()).</para>
///
/// <para><b>Key types</b>: supports any key/field type — not limited to primitives.
/// Keys can be complex objects, Slots, or any type with a registered comparer.</para>
/// </summary>
public static class TableQueryExtensions
{
    // ═══════════════════════════════════════════════════════════════════════════
    //  PRIMARY KEY QUERY ENTRY POINTS
    // ═══════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Start building a primary key range query.
    /// Chain bounds (.AtLeast, .AtMost, .Between, .GreaterThan, .LessThan),
    /// modifiers (.Take, .Skip, .Backward, .Where), then enumerate.
    /// </summary>
    public static TableQuery<TKey, TRecord> Query<TKey, TRecord>(
        this ITable<TKey, TRecord> table)
        => new(table);

    /// <summary>
    /// Start building a primary key query from a pre-built <see cref="KeyQuery{TKey}"/>.
    /// Backward-compatible bridge for existing code.
    /// </summary>
    public static TableQuery<TKey, TRecord> Query<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query)
    {
        var q = new TableQuery<TKey, TRecord>(table);
        q.SetBounds(query.From, query.HasFrom, query.FromExclusive,
                    query.To, query.HasTo, query.ToExclusive);
        if (query.Filter is { } f) q.Where(f);
        return q;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  INDEX QUERY ENTRY POINTS
    // ═══════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Start building an index query using a field selector expression.
    /// The index is resolved by matching the member name to existing indexes.
    /// </summary>
    /// <example>
    /// <code>
    /// table.Query(c => c.Email).Equals("test@test.com")
    /// table.Query(c => c.City).Equals("NYC").Take(5)
    /// </code>
    /// </example>
    public static IndexQuery<TKey, TRecord, TField> Query<TKey, TRecord, TField>(
        this ITable<TKey, TRecord> table,
        Expression<Func<TRecord, TField>> fieldSelector)
    {
        var indexName = ResolveIndexName(table, fieldSelector);
        return new IndexQuery<TKey, TRecord, TField>(table, indexName);
    }

    /// <summary>
    /// Start building an index query using an explicit index name.
    /// Use when the index name doesn't match the member name, or for composite indexes.
    /// </summary>
    public static IndexQuery<TKey, TRecord, TField> Query<TKey, TRecord, TField>(
        this ITable<TKey, TRecord> table,
        string indexName)
        => new(table, indexName);

    // ═══════════════════════════════════════════════════════════════════════════
    //  STRING-SPECIFIC EXTENSIONS
    // ═══════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Key prefix query — records whose string key starts with <paramref name="prefix"/>.
    /// Uses the WTree's O(log N) seek + tight exclusive upper bound.
    /// </summary>
    public static TableQuery<string, TRecord> StartsWith<TRecord>(
        this TableQuery<string, TRecord> query,
        string prefix)
    {
        if (string.IsNullOrEmpty(prefix))
            return query;

        var upper = KeyQuery.IncrementPrefix(prefix);
        return query.SetBounds(
            prefix, true, false,
            upper!, upper is not null, true);
    }

    /// <summary>
    /// Index prefix query — records whose indexed string field starts with <paramref name="prefix"/>.
    /// Performs a range scan on the index: [prefix, prefix+1).
    /// </summary>
    public static IndexQuery<TKey, TRecord, string> StartsWith<TKey, TRecord>(
        this IndexQuery<TKey, TRecord, string> query,
        string prefix)
    {
        if (string.IsNullOrEmpty(prefix))
            return query;

        var upper = KeyQuery.IncrementPrefix(prefix);
        return query.SetRange(
            prefix, true, true,
            upper, upper is not null, false);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  BACKWARD-COMPATIBLE HELPERS
    // ═══════════════════════════════════════════════════════════════════════════

    /// <summary>Count records in the given key range.</summary>
    public static long Count<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query)
        => table.ScanCount(query);

    /// <summary>Cursor-based paging: first page.</summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> PageAfter<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        int take)
        => table.Query(query).Take(take);

    /// <summary>
    /// Cursor-based paging: next page after <paramref name="afterKey"/> (exclusive).
    /// Always O(log N + take) regardless of depth.
    /// </summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> PageAfter<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        TKey afterKey,
        int take)
    {
        var cursor = new KeyQuery<TKey>(
            afterKey, true, true,
            query.To, query.HasTo, query.ToExclusive,
            query.Filter);
        return table.Query(cursor).Take(take);
    }

    /// <summary>Forward scan with take limit (backward-compatible).</summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> QueryTake<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        int take)
        => table.Query(query).Take(take);

    /// <summary>Backward scan (backward-compatible).</summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> QueryBackward<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query)
        => table.Query(query).Backward();

    /// <summary>Backward scan with take limit (backward-compatible).</summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> QueryBackwardTake<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        int take)
        => table.Query(query).Backward().Take(take);

    /// <summary>Offset paging (backward-compatible). Prefer cursor paging for deep pages.</summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> Page<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        int skip,
        int take)
        => table.Query(query).Skip(skip).Take(take);

    // ═══════════════════════════════════════════════════════════════════════════
    //  INTERNAL HELPERS
    // ═══════════════════════════════════════════════════════════════════════════

    private static string ResolveIndexName<TKey, TRecord, TField>(
        ITable<TKey, TRecord> table,
        Expression<Func<TRecord, TField>> fieldSelector)
    {
        var memberNames = ExtractMemberNames(fieldSelector);

        if (memberNames.Length == 1)
        {
            var candidateName = memberNames[0];
            var indexes = table.Indexes.ListIndexes();
            // Exact name match
            foreach (var idx in indexes)
                if (idx.Name == candidateName) return candidateName;
            // First-field match
            foreach (var idx in indexes)
                if (idx.MemberNames is { Length: > 0 } names && names[0] == candidateName)
                    return idx.Name;
            return candidateName;
        }

        // Composite: try concatenated name or exact member match
        var compositeName = string.Join("", memberNames);
        var allIndexes = table.Indexes.ListIndexes();
        foreach (var idx in allIndexes)
            if (idx.Name == compositeName) return compositeName;
        foreach (var idx in allIndexes)
            if (idx.MemberNames is { } names && names.Length == memberNames.Length &&
                names.SequenceEqual(memberNames))
                return idx.Name;
        return compositeName;
    }

    private static string[] ExtractMemberNames<TRecord, TField>(Expression<Func<TRecord, TField>> selector)
    {
        var body = selector.Body;

        if (body is MemberExpression member)
            return [member.Member.Name];

        if (body is NewExpression newExpr && newExpr.Arguments.Count > 0)
        {
            var names = new string[newExpr.Arguments.Count];
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                if (newExpr.Arguments[i] is MemberExpression m)
                    names[i] = m.Member.Name;
                else
                    throw new ArgumentException(
                        $"Composite selector argument {i} must be a direct member access.");
            }
            return names;
        }

        throw new ArgumentException(
            "Selector must be a member access (c => c.Email) or " +
            "anonymous type (c => new { c.City, c.Age }).");
    }
}
