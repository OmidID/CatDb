// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;

namespace CatDb.Extensions;

/// <summary>
/// Low-level primary-key helpers over the engine's native <see cref="ITable{TKey,TRecord}.Scan"/>
/// (B-tree ordered range scan + cursor paging). These are the "make life easier" wrappers around the
/// engine key scan — distinct from the field-oriented <see cref="Query{TKey,TRecord}"/> builder,
/// which is the single way to filter/sort by record fields and across indexes.
///
/// <code>
/// table.QueryTake(KeyQuery&lt;long&gt;.AtLeast(100), take: 20);   // forward range, limited
/// table.PageAfter(KeyQuery&lt;long&gt;.All(), afterKey, take: 20); // O(log N + take) cursor page
/// table.Count(KeyQuery&lt;long&gt;.Between(10, 20));               // count in range
/// </code>
/// </summary>
public static class TableKeyScanExtensions
{
    /// <summary>Count records in the given key range (engine-native count).</summary>
    public static long Count<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query)
        => table.ScanCount(query);

    /// <summary>Cursor-based paging: first page (forward range, limited to <paramref name="take"/>).</summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> PageAfter<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        int take)
        => ScanForwardTake(table, query, take);

    /// <summary>
    /// Cursor-based paging: next page after <paramref name="afterKey"/> (exclusive).
    /// Always O(log N + take) regardless of depth — the bound is pushed to the engine.
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
        return ScanForwardTake(table, cursor, take);
    }

    /// <summary>Forward scan with a row limit pushed to the engine.</summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> QueryTake<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        int take)
        => ScanForwardTake(table, query, take);

    /// <summary>Backward scan over the key range.</summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> QueryBackward<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query)
        => table.ScanBackward(query);

    /// <summary>Backward scan with a row limit pushed to the engine.</summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> QueryBackwardTake<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        int take)
        => ScanBackwardTake(table, query, take);

    /// <summary>Offset paging. Prefer cursor paging (<see cref="PageAfter{TKey,TRecord}(ITable{TKey,TRecord},KeyQuery{TKey},TKey,int)"/>) for deep pages.</summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> Page<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        int skip,
        int take)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(skip);
        ArgumentOutOfRangeException.ThrowIfNegative(take);
        IEnumerable<KeyValuePair<TKey, TRecord>> src = ScanForwardTake(table, query, skip + take);
        return src.Skip(skip).Take(take);
    }

    // ── Engine take-pushdown dispatch (concrete tables limit at the source) ─────

    private static IEnumerable<KeyValuePair<TKey, TRecord>> ScanForwardTake<TKey, TRecord>(
        ITable<TKey, TRecord> table, KeyQuery<TKey> query, int take)
    {
        if (table is XTable<TKey, TRecord> xt) return xt.ScanTake(query, take);
        if (table is XTablePortable<TKey, TRecord> xp) return xp.ScanTake(query, take);
        return table.Scan(query).Take(take);
    }

    private static IEnumerable<KeyValuePair<TKey, TRecord>> ScanBackwardTake<TKey, TRecord>(
        ITable<TKey, TRecord> table, KeyQuery<TKey> query, int take)
    {
        if (table is XTable<TKey, TRecord> xt) return xt.ScanBackwardTake(query, take);
        if (table is XTablePortable<TKey, TRecord> xp) return xp.ScanBackwardTake(query, take);
        return table.ScanBackward(query).Take(take);
    }
}
