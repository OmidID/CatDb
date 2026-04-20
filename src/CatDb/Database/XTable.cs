using System.Collections;
using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public class XTable<TKey, TRecord>(ITable<IData, IData> table) : ITable<TKey, TRecord>
{
    public ITable<IData, IData> Table { get; } = table ?? throw new ArgumentNullException(nameof(table));

    private static IData K(TKey key)       => new Data<TKey>(key);
    private static IData R(TRecord record) => new Data<TRecord>(record);
    private static TKey   FromK(IData d)   => ((Data<TKey>)d).Value;
    private static TRecord FromR(IData d)  => ((Data<TRecord>)d).Value;

    private static KeyValuePair<TKey, TRecord> Pair(KeyValuePair<IData, IData> kv) =>
        new(FromK(kv.Key), FromR(kv.Value));

    private static KeyValuePair<TKey, TRecord>? Nullable(KeyValuePair<IData, IData>? kv) =>
        kv is null ? null : Pair(kv.Value);

    public TRecord this[TKey key]
    {
        get => FromR(Table[K(key)]);
        set => Table[K(key)] = R(value);
    }

    public void Replace(TKey key, TRecord record)         => Table.Replace(K(key), R(record));
    public void InsertOrIgnore(TKey key, TRecord record)  => Table.InsertOrIgnore(K(key), R(record));
    public void Delete(TKey key)                          => Table.Delete(K(key));
    public void Delete(TKey fromKey, TKey toKey)          => Table.Delete(K(fromKey), K(toKey));
    public void Clear()                                   => Table.Clear();
    public bool Exists(TKey key)                          => Table.Exists(K(key));
    public long Count()                                   => Table.Count();
    public IDescriptor Descriptor                         => Table.Descriptor;

    public bool TryGet(TKey key, out TRecord record)
    {
        if (!Table.TryGet(K(key), out var irec))
        {
            record = default;
            return false;
        }
        record = FromR(irec);
        return true;
    }

    public TRecord Find(TKey key)
    {
        var irec = Table.Find(K(key));
        return irec is null ? default : FromR(irec);
    }

    public TRecord TryGetOrDefault(TKey key, TRecord defaultRecord) =>
        FromR(Table.TryGetOrDefault(K(key), R(defaultRecord)));

    public KeyValuePair<TKey, TRecord>? FindNext(TKey key)   => Nullable(Table.FindNext(K(key)));
    public KeyValuePair<TKey, TRecord>? FindAfter(TKey key)  => Nullable(Table.FindAfter(K(key)));
    public KeyValuePair<TKey, TRecord>? FindPrev(TKey key)   => Nullable(Table.FindPrev(K(key)));
    public KeyValuePair<TKey, TRecord>? FindBefore(TKey key) => Nullable(Table.FindBefore(K(key)));

    public IEnumerable<KeyValuePair<TKey, TRecord>> Forward()
    {
        foreach (var kv in Table.Forward()) yield return Pair(kv);
    }

    public IEnumerable<KeyValuePair<TKey, TRecord>> Forward(TKey from, bool hasFrom, TKey to, bool hasTo)
    {
        var ifrom = hasFrom ? K(from) : null;
        var ito   = hasTo   ? K(to)   : null;
        foreach (var kv in Table.Forward(ifrom, hasFrom, ito, hasTo)) yield return Pair(kv);
    }

    public IEnumerable<KeyValuePair<TKey, TRecord>> Backward()
    {
        foreach (var kv in Table.Backward()) yield return Pair(kv);
    }

    public IEnumerable<KeyValuePair<TKey, TRecord>> Backward(TKey to, bool hasTo, TKey from, bool hasFrom)
    {
        var ito   = hasTo   ? K(to)   : null;
        var ifrom = hasFrom ? K(from) : null;
        foreach (var kv in Table.Backward(ito, hasTo, ifrom, hasFrom)) yield return Pair(kv);
    }

    public KeyValuePair<TKey, TRecord> FirstRow => Pair(Table.FirstRow);
    public KeyValuePair<TKey, TRecord> LastRow  => Pair(Table.LastRow);

    public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator() => Forward().GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator()                         => GetEnumerator();
}
