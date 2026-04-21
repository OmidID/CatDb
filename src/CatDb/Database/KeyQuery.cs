#pragma warning disable CS8618, CS8604
namespace CatDb.Database;

/// <summary>
/// Describes a range/predicate query over a sorted key space, exploiting the
/// WTree's O(log N) seek + sequential scan.  Construct via the static factory
/// methods; for string keys use the companion <see cref="KeyQuery"/> static class.
///
/// Design note: <see cref="Filter"/> is intentionally reserved (unused by the
/// engine today) so that a future value-index feature can attach arbitrary
/// key-level predicates without breaking callers.
/// </summary>
public readonly struct KeyQuery<TKey>
{
    /// <summary>Lower bound passed to the WTree engine (inclusive). Only meaningful when <see cref="HasFrom"/> is true.</summary>
    public readonly TKey  From;
    public readonly bool  HasFrom;

    /// <summary>Upper bound passed to the WTree engine (inclusive). Only meaningful when <see cref="HasTo"/> is true.</summary>
    public readonly TKey  To;
    public readonly bool  HasTo;

    /// <summary>
    /// Forward: skip leading keys while predicate is true (implements exclusive lower bound).
    /// Backward: automatically becomes a stop condition — see <see cref="TableQueryExtensions.QueryBackward{TKey,TRecord}"/>.
    /// </summary>
    public readonly Func<TKey, bool>? SkipWhile;

    /// <summary>
    /// Forward: stop enumeration when predicate returns false (implements exclusive upper bound / prefix boundary).
    /// Backward: automatically becomes a leading-skip + stop condition.
    /// </summary>
    public readonly Func<TKey, bool>? TakeWhile;

    /// <summary>
    /// Reserved for future value-index use: arbitrary key-level post-scan predicate.
    /// Attach via <see cref="WithFilter"/>.
    /// </summary>
    public readonly Func<TKey, bool>? Filter;

    internal KeyQuery(TKey from, bool hasFrom, TKey to, bool hasTo,
                      Func<TKey, bool>? skipWhile, Func<TKey, bool>? takeWhile,
                      Func<TKey, bool>? filter)
    {
        From      = from;
        HasFrom   = hasFrom;
        To        = to;
        HasTo     = hasTo;
        SkipWhile = skipWhile;
        TakeWhile = takeWhile;
        Filter    = filter;
    }

    private static readonly EqualityComparer<TKey> Eq = EqualityComparer<TKey>.Default;

    /// <summary>Returns all records — no bounds applied.</summary>
    public static KeyQuery<TKey> All() =>
        new(default!, false, default!, false, null, null, null);

    /// <summary>Returns records where key &gt;= <paramref name="from"/>.</summary>
    public static KeyQuery<TKey> AtLeast(TKey from) =>
        new(from, true, default!, false, null, null, null);

    /// <summary>Returns records where key &gt; <paramref name="from"/> (exclusive).</summary>
    public static KeyQuery<TKey> GreaterThan(TKey from) =>
        new(from, true, default!, false,
            k => Eq.Equals(k, from), null, null);

    /// <summary>Returns records where key &lt;= <paramref name="to"/>.</summary>
    public static KeyQuery<TKey> AtMost(TKey to) =>
        new(default!, false, to, true, null, null, null);

    /// <summary>Returns records where key &lt; <paramref name="to"/> (exclusive).</summary>
    public static KeyQuery<TKey> LessThan(TKey to) =>
        new(default!, false, to, true,
            null, k => !Eq.Equals(k, to), null);

    /// <summary>
    /// Returns records where key is in [<paramref name="from"/>, <paramref name="to"/>].
    /// Set <paramref name="fromInclusive"/> or <paramref name="toInclusive"/> to false
    /// for open (exclusive) bounds.
    /// </summary>
    public static KeyQuery<TKey> Between(TKey from, TKey to,
                                          bool fromInclusive = true, bool toInclusive = true) =>
        new(from, true, to, true,
            fromInclusive ? null : (Func<TKey, bool>)(k => Eq.Equals(k, from)),
            toInclusive   ? null : (Func<TKey, bool>)(k => !Eq.Equals(k, to)),
            null);

    /// <summary>
    /// Attach an arbitrary key-level post-scan predicate.
    /// Reserved for future value-index features; the engine does not push this down.
    /// </summary>
    public KeyQuery<TKey> WithFilter(Func<TKey, bool> filter) =>
        new(From, HasFrom, To, HasTo, SkipWhile, TakeWhile, filter);
}

/// <summary>
/// String-specific key query factories.
/// </summary>
public static class KeyQuery
{
    /// <summary>
    /// Returns all records whose string key starts with <paramref name="prefix"/> using
    /// ordinal comparison.  The WTree seek jumps directly to the first key &gt;= prefix;
    /// scanning stops at the first key that no longer starts with the prefix.
    /// Cost: O(log N) seek + O(matches) scan — no full-table scan.
    /// Works with both <c>Query</c> (forward) and <c>QueryBackward</c> (reverse).
    /// </summary>
    public static KeyQuery<string> StartsWith(string prefix) =>
        new KeyQuery<string>(prefix, true, default!, false,
            null,
            k => k.StartsWith(prefix, StringComparison.Ordinal),
            null);
}
