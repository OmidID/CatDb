// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8618, CS8604
namespace CatDb.Database;

/// <summary>
/// Describes a range query over a sorted key space, exploiting the WTree's
/// O(log N) seek + sequential leaf scan.
///
/// <para>
/// All bound handling — inclusive, exclusive, prefix — is encoded as exact
/// <see cref="From"/> / <see cref="To"/> keys plus <see cref="FromExclusive"/> /
/// <see cref="ToExclusive"/> flags.  These values are passed directly into the
/// WTree leaf iterator (<c>IOrderedSet.ForwardExclusive</c> /
/// <c>BackwardExclusive</c>), where the binary-search start/stop positions are
/// adjusted with a single index arithmetic operation — no delegate call per record.
/// </para>
///
/// <para>
/// For <c>StartsWith</c> queries the engine computes a tight exclusive upper bound
/// (e.g. prefix "abc" → upper bound "abd") so the WTree stops loading leaf nodes
/// as soon as the prefix range is exhausted — no extra pages are loaded.
/// </para>
/// </summary>
public readonly struct KeyQuery<TKey>
{
    /// <summary>Lower seek key passed directly to the WTree.  Meaningful when <see cref="HasFrom"/> is true.</summary>
    public readonly TKey From;
    public readonly bool HasFrom;

    /// <summary>
    /// When <c>true</c> the record whose key equals <see cref="From"/> is excluded
    /// (GreaterThan semantics).  The engine still seeks to <see cref="From"/> in
    /// O(log N) and then skips the exact match inside the leaf via index arithmetic —
    /// no delegate call.
    /// </summary>
    public readonly bool FromExclusive;

    /// <summary>Upper seek key passed directly to the WTree.  Meaningful when <see cref="HasTo"/> is true.</summary>
    public readonly TKey To;
    public readonly bool HasTo;

    /// <summary>
    /// When <c>true</c> the record whose key equals <see cref="To"/> is excluded
    /// (LessThan / open-right semantics).  For <c>StartsWith</c> queries this is
    /// always <c>true</c> and <see cref="To"/> is set to the first key that cannot
    /// start with the prefix — the leaf early-exit fires without any string comparison
    /// per record.
    /// </summary>
    public readonly bool ToExclusive;

    /// <summary>
    /// Optional post-scan key predicate applied inside the engine's tight
    /// <c>foreach</c> loop — one direct call per record, no LINQ delegate chain.
    /// Cannot be pushed into the WTree (the index is key-ordered only).
    /// Attach via <see cref="WithFilter"/>.
    /// </summary>
    public readonly Func<TKey, bool>? Filter;

    internal KeyQuery(TKey from, bool hasFrom, bool fromExclusive,
                      TKey to,   bool hasTo,   bool toExclusive,
                      Func<TKey, bool>? filter)
    {
        From          = from;
        HasFrom       = hasFrom;
        FromExclusive = fromExclusive;
        To            = to;
        HasTo         = hasTo;
        ToExclusive   = toExclusive;
        Filter        = filter;
    }

    /// <summary>Returns all records — no bounds.</summary>
    public static KeyQuery<TKey> All() =>
        new(default!, false, false, default!, false, false, null);

    /// <summary>Records where key &gt;= <paramref name="from"/>.</summary>
    public static KeyQuery<TKey> AtLeast(TKey from) =>
        new(from, true, false, default!, false, false, null);

    /// <summary>Records where key &gt; <paramref name="from"/> (exclusive lower bound).</summary>
    public static KeyQuery<TKey> GreaterThan(TKey from) =>
        new(from, true, true, default!, false, false, null);

    /// <summary>Records where key &lt;= <paramref name="to"/>.</summary>
    public static KeyQuery<TKey> AtMost(TKey to) =>
        new(default!, false, false, to, true, false, null);

    /// <summary>Records where key &lt; <paramref name="to"/> (exclusive upper bound).</summary>
    public static KeyQuery<TKey> LessThan(TKey to) =>
        new(default!, false, false, to, true, true, null);

    /// <summary>
    /// Records where key is in [<paramref name="from"/>, <paramref name="to"/>].
    /// Set <paramref name="fromInclusive"/> or <paramref name="toInclusive"/> to
    /// <c>false</c> for open (exclusive) endpoints.
    /// </summary>
    public static KeyQuery<TKey> Between(TKey from, TKey to,
                                          bool fromInclusive = true, bool toInclusive = true) =>
        new(from, true, !fromInclusive, to, true, !toInclusive, null);

    /// <summary>Attach an arbitrary post-scan key predicate (e.g. an even-only filter).</summary>
    public KeyQuery<TKey> WithFilter(Func<TKey, bool> filter) =>
        new(From, HasFrom, FromExclusive, To, HasTo, ToExclusive, filter);
}

/// <summary>String-specific key query factories.</summary>
public static class KeyQuery
{
    /// <summary>
    /// Returns all records whose string key starts with <paramref name="prefix"/>
    /// using ordinal comparison.
    ///
    /// <para>
    /// The WTree seek jumps to the first key &gt;= prefix in O(log N).
    /// The exclusive upper bound is computed as the first string lexicographically
    /// greater than any string starting with the prefix (e.g. "abc" → "abd").
    /// Both bounds are passed directly into the WTree engine: no string predicate is
    /// evaluated per record, and no leaf pages beyond the prefix range are loaded.
    /// </para>
    /// </summary>
    public static KeyQuery<string> StartsWith(string prefix)
    {
        if (string.IsNullOrEmpty(prefix))
            return KeyQuery<string>.All();

        var upper = IncrementPrefix(prefix);
        return new KeyQuery<string>(
            prefix, true,  false,             // inclusive lower bound
            upper!,  upper is not null, true,  // exclusive upper bound (null = no bound)
            null);
    }

    /// <summary>
    /// Returns the smallest string that is strictly greater than every string
    /// starting with <paramref name="prefix"/>.  Returns <c>null</c> when all
    /// characters in the prefix are <see cref="char.MaxValue"/> and no such string
    /// exists.
    /// </summary>
    internal static string? IncrementPrefix(string prefix)
    {
        var chars = prefix.ToCharArray();
        for (var i = chars.Length - 1; i >= 0; i--)
        {
            if (chars[i] < char.MaxValue)
            {
                chars[i]++;
                return new string(chars, 0, i + 1); // truncate: any suffix was \uFFFF
            }
        }
        return null; // all chars were \uFFFF → no upper bound possible
    }
}
