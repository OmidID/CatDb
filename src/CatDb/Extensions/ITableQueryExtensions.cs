using CatDb.Database;

namespace CatDb.Extensions;

/// <summary>
/// Performance summary for callers that need to reason about scale:
///
///   Query / QueryBackward   — O(log N) seek + O(M) scan (M = records returned). Fast.
///   Count(query)            — O(M) scan of the matching range.  No subtree sizes in WTree.
///   Page(query,skip,take)   — O(skip) scan from range start.  Avoid for deep pages on huge data.
///   PageAfter(key,take)     — O(log N) per call, always.  Preferred for large datasets.
///
/// For 10M+ records with pagination use PageAfter (keyset/cursor pagination).
/// Store the last key of each page and pass it as the next cursor — no offset scanning needed.
/// </summary>
public static class TableQueryExtensions
{
    /// <summary>
    /// Scans the table <b>forward</b> (ascending) using WTree's sorted seek,
    /// then applies the predicates in <paramref name="query"/> lazily.
    ///
    /// <list type="bullet">
    ///   <item>Engine seeks to <c>From</c> / <c>To</c> (both inclusive) — O(log N).</item>
    ///   <item><c>SkipWhile</c> discards the first key(s) for exclusive lower bounds.</item>
    ///   <item><c>TakeWhile</c> stops scan for exclusive upper bounds or prefix limits.</item>
    ///   <item><c>Filter</c> is a reserved post-scan predicate (future value-index hook).</item>
    /// </list>
    ///
    /// Example usage:
    /// <code>
    ///   table.Query(KeyQuery&lt;int&gt;.GreaterThan(5))
    ///   table.Query(KeyQuery&lt;int&gt;.Between(3, 7, fromInclusive: false))
    ///   table.Query(KeyQuery.StartsWith("abc"))
    /// </code>
    /// </summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> Query<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query)
    {
        var seq = table.Forward(query.From, query.HasFrom, query.To, query.HasTo);

        if (query.SkipWhile is { } sw)
            seq = seq.SkipWhile(kv => sw(kv.Key));

        if (query.TakeWhile is { } tw)
            seq = seq.TakeWhile(kv => tw(kv.Key));

        if (query.Filter is { } f)
            seq = seq.Where(kv => f(kv.Key));

        return seq;
    }

    /// <summary>
    /// Scans the table <b>backward</b> (descending) using WTree's sorted seek,
    /// then applies the predicates in <paramref name="query"/> lazily.
    ///
    /// <para>
    /// The <c>SkipWhile</c> / <c>TakeWhile</c> predicates are automatically adapted for
    /// the high→low direction, so queries built with the forward factory methods
    /// (e.g. <c>GreaterThan</c>, <c>LessThan</c>, <c>Between</c>, <c>StartsWith</c>)
    /// work correctly in both directions without modification.
    /// </para>
    ///
    /// Predicate inversion rules:
    /// <list type="bullet">
    ///   <item><c>TakeWhile</c> → first <c>SkipWhile(!tw)</c> (skip keys above range at the top),
    ///         then <c>TakeWhile(tw)</c> (stop when range is exited below).</item>
    ///   <item><c>SkipWhile</c> → <c>TakeWhile(!sw)</c> (stop at exclusive lower bound).</item>
    /// </list>
    /// </summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> QueryBackward<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query)
    {
        // Backward(to, hasTo, from, hasFrom) — descending from upper to lower bound.
        var seq = table.Backward(query.To, query.HasTo, query.From, query.HasFrom);

        // Forward's TakeWhile (exclusive upper / range continuation) maps to:
        //   1. Skip leading out-of-range keys at the top  (e.g. for StartsWith: skip keys before prefix)
        //   2. Stop when we exit the range below           (e.g. for StartsWith: stop when prefix ends)
        if (query.TakeWhile is { } tw)
        {
            seq = seq.SkipWhile(kv => !tw(kv.Key)); // skip keys above the range
            seq = seq.TakeWhile(kv =>  tw(kv.Key)); // stop when below the range
        }

        // Forward's SkipWhile (exclusive lower bound) maps to:
        //   Stop when we reach the exclusive lower bound key (it would have been skipped in forward).
        if (query.SkipWhile is { } sw)
            seq = seq.TakeWhile(kv => !sw(kv.Key));

        if (query.Filter is { } f)
            seq = seq.Where(kv => f(kv.Key));

        return seq;
    }

    // -------------------------------------------------------------------------
    // Count
    // -------------------------------------------------------------------------

    /// <summary>
    /// Counts records matching <paramref name="query"/>.
    /// Cost: O(M) scan of the matching range — the WTree has no subtree-size index.
    /// For total-page calculations on huge datasets consider caching this value.
    /// </summary>
    public static long Count<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query)
    {
        long n = 0;
        foreach (var _ in table.Query(query)) n++;
        return n;
    }

    // -------------------------------------------------------------------------
    // Offset paging  (simple but O(skip) — avoid for deep pages on large tables)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Returns one page of records using offset/skip paging.
    /// <para>
    /// Cost: O(log N) seek to range start + O(skip) scan to reach the page offset.
    /// Acceptable for small tables or shallow pages.
    /// For deep pages on 10M+ records use <see cref="PageAfter{TKey,TRecord}"/> instead.
    /// </para>
    /// </summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> Page<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        int skip,
        int take)
    {
        if (skip < 0)  throw new ArgumentOutOfRangeException(nameof(skip));
        if (take <= 0) throw new ArgumentOutOfRangeException(nameof(take));
        return table.Query(query).Skip(skip).Take(take);
    }

    // -------------------------------------------------------------------------
    // Keyset / cursor paging  (always O(log N) regardless of page depth)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Returns the <b>first</b> page of records matching <paramref name="query"/>
    /// using keyset (cursor) pagination.  Always O(log N + take).
    ///
    /// <para>Pass the last key of each page to
    /// <see cref="PageAfter{TKey,TRecord}(ITable{TKey,TRecord},KeyQuery{TKey},TKey,int)"/>
    /// to fetch the next page.</para>
    /// </summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> PageAfter<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        int take)
    {
        if (take <= 0) throw new ArgumentOutOfRangeException(nameof(take));
        return table.Query(query).Take(take);
    }

    /// <summary>
    /// Returns the <b>next</b> page of records after <paramref name="afterKey"/> (exclusive),
    /// matching <paramref name="query"/>, using keyset (cursor) pagination.
    /// Always O(log N + take) regardless of how many pages have been fetched.
    ///
    /// <code>
    ///   // First page
    ///   var page = table.PageAfter(query, take: 20).ToList();
    ///
    ///   // Subsequent pages — cursor is the last key of previous page
    ///   var next = table.PageAfter(query, afterKey: page.Last().Key, take: 20).ToList();
    /// </code>
    ///
    /// Recommended for 10M+ record tables.
    /// Total page count requires <see cref="Count{TKey,TRecord}"/> (O(M) — cache it).
    /// </summary>
    public static IEnumerable<KeyValuePair<TKey, TRecord>> PageAfter<TKey, TRecord>(
        this ITable<TKey, TRecord> table,
        KeyQuery<TKey> query,
        TKey afterKey,
        int take)
    {
        if (take <= 0) throw new ArgumentOutOfRangeException(nameof(take));

        var eq = EqualityComparer<TKey>.Default;

        // Narrow the lower bound to strictly after the cursor key,
        // preserving any existing upper bound + predicates.
        var effective = new KeyQuery<TKey>(
            afterKey, true,
            query.To, query.HasTo,
            k => eq.Equals(k, afterKey),   // SkipWhile: skip the cursor key itself
            query.TakeWhile,
            query.Filter);

        return table.Query(effective).Take(take);
    }
}
