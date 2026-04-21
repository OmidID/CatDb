using System.Diagnostics.CodeAnalysis;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public interface ITable { }

public interface ITable<TKey, TRecord> : ITable, IEnumerable<KeyValuePair<TKey, TRecord>>
{
    TRecord this[TKey key] { get; set; }

    void Replace(TKey key, TRecord record);
    void InsertOrIgnore(TKey key, TRecord record);
    void Delete(TKey key);
    void Delete(TKey fromKey, TKey toKey);
    void Clear();

    bool Exists(TKey key);
    bool TryGet(TKey key, [NotNullWhen(true)] out TRecord? record);
    TRecord? Find(TKey key);
    TRecord TryGetOrDefault(TKey key, TRecord defaultRecord);

    KeyValuePair<TKey, TRecord>? FindNext(TKey key);
    KeyValuePair<TKey, TRecord>? FindAfter(TKey key);
    KeyValuePair<TKey, TRecord>? FindPrev(TKey key);
    KeyValuePair<TKey, TRecord>? FindBefore(TKey key);

    IEnumerable<KeyValuePair<TKey, TRecord>> Forward();
    IEnumerable<KeyValuePair<TKey, TRecord>> Forward(TKey from, bool hasFrom, TKey to, bool hasTo);
    IEnumerable<KeyValuePair<TKey, TRecord>> Backward();
    IEnumerable<KeyValuePair<TKey, TRecord>> Backward(TKey to, bool hasTo, TKey from, bool hasFrom);

    /// <summary>
    /// Engine-native forward scan with inclusive/exclusive bounds from <paramref name="query"/>.
    /// Default: delegates to <see cref="Forward"/> with a minimal per-endpoint adjustment loop.
    /// Override in concrete table classes for zero-overhead WTree leaf-level iteration.
    /// </summary>
    IEnumerable<KeyValuePair<TKey, TRecord>> Scan(KeyQuery<TKey> query)
    {
        var eq = System.Collections.Generic.EqualityComparer<TKey>.Default;
        bool skipFirst = query.FromExclusive && query.HasFrom;
        foreach (var kv in Forward(query.From, query.HasFrom, query.To, query.HasTo))
        {
            if (skipFirst) { skipFirst = false; if (eq.Equals(kv.Key, query.From)) continue; }
            if (query.ToExclusive && query.HasTo && eq.Equals(kv.Key, query.To)) break;
            if (query.Filter is { } f && !f(kv.Key)) continue;
            yield return kv;
        }
    }

    /// <summary>
    /// Engine-native backward scan with inclusive/exclusive bounds from <paramref name="query"/>.
    /// Default: delegates to <see cref="Backward"/> with a minimal per-endpoint adjustment loop.
    /// Override in concrete table classes for zero-overhead WTree leaf-level iteration.
    /// </summary>
    IEnumerable<KeyValuePair<TKey, TRecord>> ScanBackward(KeyQuery<TKey> query)
    {
        var eq = System.Collections.Generic.EqualityComparer<TKey>.Default;
        bool skipFirst = query.ToExclusive && query.HasTo;
        foreach (var kv in Backward(query.To, query.HasTo, query.From, query.HasFrom))
        {
            if (skipFirst) { skipFirst = false; if (eq.Equals(kv.Key, query.To)) continue; }
            if (query.FromExclusive && query.HasFrom && eq.Equals(kv.Key, query.From)) break;
            if (query.Filter is { } f && !f(kv.Key)) continue;
            yield return kv;
        }
    }

    /// <summary>
    /// Counts records matching <paramref name="query"/> using engine-native index arithmetic.
    /// Default: iterates via <see cref="Scan"/>.  Override for O(leaves × log leafSize).
    /// </summary>
    long ScanCount(KeyQuery<TKey> query)
    {
        long n = 0;
        foreach (var _ in Scan(query)) n++;
        return n;
    }

    KeyValuePair<TKey, TRecord>? FirstRow { get; }
    KeyValuePair<TKey, TRecord>? LastRow  { get; }

    IDescriptor Descriptor { get; }

    long Count();
}