#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using System.Collections;
using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public class XTablePortable<TKey, TRecord> : ITable<TKey, TRecord>
{
    public ITable<IData, IData>         Table             { get; }
    public ITransformer<TKey,    IData> KeyTransformer    { get; }
    public ITransformer<TRecord, IData> RecordTransformer { get; }

    public XTablePortable(
        ITable<IData, IData> table,
        ITransformer<TKey,    IData> keyTransformer    = null,
        ITransformer<TRecord, IData> recordTransformer = null)
    {
        Table             = table ?? throw new ArgumentNullException(nameof(table));
        KeyTransformer    = keyTransformer    ?? new DataTransformer<TKey>(table.Descriptor.KeyType);
        RecordTransformer = recordTransformer ?? new DataTransformer<TRecord>(table.Descriptor.RecordType);
    }

    private KeyValuePair<TKey, TRecord> Pair(KeyValuePair<IData, IData> kv) =>
        new(KeyTransformer.From(kv.Key), RecordTransformer.From(kv.Value));

    private KeyValuePair<TKey, TRecord>? Nullable(KeyValuePair<IData, IData>? kv) =>
        kv is null ? null : Pair(kv.Value);

    public TRecord this[TKey key]
    {
        get => RecordTransformer.From(Table[KeyTransformer.To(key)]);
        set => Table[KeyTransformer.To(key)] = RecordTransformer.To(value);
    }

    public void Replace(TKey key, TRecord record)        => Table.Replace(KeyTransformer.To(key), RecordTransformer.To(record));
    public void InsertOrIgnore(TKey key, TRecord record) => Table.InsertOrIgnore(KeyTransformer.To(key), RecordTransformer.To(record));
    public void Delete(TKey key)                         => Table.Delete(KeyTransformer.To(key));
    public void Delete(TKey fromKey, TKey toKey)         => Table.Delete(KeyTransformer.To(fromKey), KeyTransformer.To(toKey));
    public void Clear()                                  => Table.Clear();
    public bool Exists(TKey key)                         => Table.Exists(KeyTransformer.To(key));
    public long Count()                                  => Table.Count();
    public IDescriptor Descriptor                        => Table.Descriptor;

    public bool TryGet(TKey key, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TRecord? record)
    {
        if (!Table.TryGet(KeyTransformer.To(key), out var irec))
        {
            record = default;
            return false;
        }
        record = RecordTransformer.From(irec)!;
        return true;
    }

    public TRecord Find(TKey key)
    {
        var irec = Table.Find(KeyTransformer.To(key));
        return irec is null ? default : RecordTransformer.From(irec);
    }

    public TRecord TryGetOrDefault(TKey key, TRecord defaultRecord) =>
        RecordTransformer.From(Table.TryGetOrDefault(KeyTransformer.To(key), RecordTransformer.To(defaultRecord)));

    public KeyValuePair<TKey, TRecord>? FindNext(TKey key)   => Nullable(Table.FindNext(KeyTransformer.To(key)));
    public KeyValuePair<TKey, TRecord>? FindAfter(TKey key)  => Nullable(Table.FindAfter(KeyTransformer.To(key)));
    public KeyValuePair<TKey, TRecord>? FindPrev(TKey key)   => Nullable(Table.FindPrev(KeyTransformer.To(key)));
    public KeyValuePair<TKey, TRecord>? FindBefore(TKey key) => Nullable(Table.FindBefore(KeyTransformer.To(key)));

    public IEnumerable<KeyValuePair<TKey, TRecord>> Forward()
    {
        foreach (var kv in Table.Forward()) yield return Pair(kv);
    }

    public IEnumerable<KeyValuePair<TKey, TRecord>> Forward(TKey from, bool hasFrom, TKey to, bool hasTo)
    {
        var ifrom = hasFrom ? KeyTransformer.To(from) : null;
        var ito   = hasTo   ? KeyTransformer.To(to)   : null;
        foreach (var kv in Table.Forward(ifrom, hasFrom, ito, hasTo)) yield return Pair(kv);
    }

    public IEnumerable<KeyValuePair<TKey, TRecord>> Backward()
    {
        foreach (var kv in Table.Backward()) yield return Pair(kv);
    }

    public IEnumerable<KeyValuePair<TKey, TRecord>> Backward(TKey to, bool hasTo, TKey from, bool hasFrom)
    {
        var ito   = hasTo   ? KeyTransformer.To(to)   : null;
        var ifrom = hasFrom ? KeyTransformer.To(from)  : null;
        foreach (var kv in Table.Backward(ito, hasTo, ifrom, hasFrom)) yield return Pair(kv);
    }

    public KeyValuePair<TKey, TRecord>? FirstRow => Table.FirstRow is { } f ? Pair(f) : null;
    public KeyValuePair<TKey, TRecord>? LastRow  => Table.LastRow  is { } l ? Pair(l) : null;

    public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator() => Forward().GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator()                         => GetEnumerator();
}
