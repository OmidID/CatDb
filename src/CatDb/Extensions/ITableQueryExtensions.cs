using CatDb.Database;

namespace CatDb.Extensions;

/// <summary>
/// Convenience API over <see cref="ITable{TKey,TRecord}.Scan"/> /
/// <see cref="ITable{TKey,TRecord}.ScanBackward"/> — the two engine-native methods
/// that resolve all bound semantics inside the WTree leaf iterator.
///
/// Performance summary:
///   Query / QueryBackward   — O(log N) seek + O(M) scan.
///                             Bounds handled inside <c>IOrderedSet.ForwardExclusive</c>
///                             / <c>BackwardExclusive</c> via index arithmetic, no delegates.
///   Count(query)            — O(M) scan; the WTree has no subtree-size counters.
///   Page(query,skip,take)   — O(skip) scan from range start.
///                             Avoid for deep pages on large data.
///   PageAfter(take)         — O(log N + take), first page.
///   PageAfter(afterKey,take)— O(log N + take), always — preferred for deep paging.
/// </summary>
public static class TableQueryExtensions
{
    /// <summary>
    /// Scans the table <b>forward</b> (ascending) using the WTree engine's native
    /// bounded leaf iterator.  All bound semantics (inclusive / exclusive endpoints,
    /// prefix upper bound for <see cref="KeyQuery.StartsWith"/>) are resolved inside
    /// <c>IOrderedSet.ForwardExclusive</c> — no LINQ delegate chain, no per-record
    /// predicate call for boundary keys.
    /// </summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> Query<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query)
        => table.Scan(query);

    /// <summary>
    /// Scans the table <b>backward</b> (descending) using the WTree engine's native
    /// bounded leaf iterator.  All bound semantics are resolved inside
    /// <c>IOrderedSet.BackwardExclusive</c>.
    /// </summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> QueryBackward<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query)
        => table.ScanBackward(query);

    // ─── Count ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Counts records matching <paramref name="query"/> using engine-native
    /// index arithmetic.
    ///
    /// When the WTree leaf is in sorted-list mode (the normal case), the count
    /// is computed via <c>TryGetSortedRange</c> — two binary searches per leaf,
    /// zero per-record work.  For 2M records across ~62 leaves this takes
    /// microseconds instead of the hundreds of milliseconds that record iteration
    /// would require.
    /// </summary>
    public static long Count<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query)
        => table.ScanCount(query);

    // ─── Offset paging ───────────────────────────────────────────────────────

    /// <summary>
    /// Returns one page using offset/skip paging.
    /// Cost: O(log N) seek + O(skip) scan.  Avoid for deep pages on large tables;
    /// use <see cref="PageAfter{TKey,TRecord}(ITable{TKey,TRecord},KeyQuery{TKey},TKey,int)"/>
    /// instead.
    /// </summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> Page<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        int skip,
        int take)
    {
        if (skip < 0)  throw new ArgumentOutOfRangeException(nameof(skip));
        if (take <= 0) throw new ArgumentOutOfRangeException(nameof(take));
        var i = 0;
        foreach (var kv in table.Scan(query))
        {
            if (i++ < skip) continue;
            yield return kv;
            if (--take == 0) yield break;
        }
    }

    // ─── Keyset / cursor paging ───────────────────────────────────────────────

    /// <summary>
    /// Returns the <b>first</b> page matching <paramref name="query"/>.
    /// Always O(log N + take) — no offset scanning.
    /// </summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> PageAfter<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        int take)
    {
        if (take <= 0) throw new ArgumentOutOfRangeException(nameof(take));
        var n = 0;
        foreach (var kv in table.Scan(query))
        {
            yield return kv;
            if (++n == take) yield break;
        }
    }

    /// <summary>
    /// Returns the <b>next</b> page after <paramref name="afterKey"/> (exclusive),
    /// always O(log N + take) regardless of page depth.
    ///
    /// The engine seeks directly to <paramref name="afterKey"/> and skips it via
    /// <c>FromExclusive = true</c> — the index arithmetic inside
    /// <c>IOrderedSet.ForwardExclusive</c> handles the skip with no delegate call.
    ///
    /// <code>
    ///   var page1 = table.PageAfter(query, take: 20).ToList();
    ///   var page2 = table.PageAfter(query, afterKey: page1.Last().Key, take: 20).ToList();
    /// </code>
    /// </summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> PageAfter<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        TKey afterKey,
        int take)
    {
        if (take <= 0) throw new ArgumentOutOfRangeException(nameof(take));

        // Build an exclusive-lower-bound query starting strictly after the cursor key.
        // FromExclusive=true tells the engine to seek to afterKey and adjust the leaf
        // iterator start index by one — no EqualityComparer delegate needed.
        var cursor = new KeyQuery<TKey>(
            afterKey, true, true,                         // exclusive lower: skip afterKey itself
            query.To, query.HasTo, query.ToExclusive,     // preserve original upper bound
            query.Filter);

        var n = 0;
        foreach (var kv in table.Scan(cursor))
        {
            yield return kv;
            if (++n == take) yield break;
        }
    }
}
