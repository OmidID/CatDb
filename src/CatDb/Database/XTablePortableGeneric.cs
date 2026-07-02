// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using System.Collections;
using CatDb.Data;
using CatDb.Database.Indexing;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public class XTablePortable<TKey, TRecord> : ITable<TKey, TRecord>
{
    public ITable<IData, IData>         Table             { get; }
    public ITransformer<TKey,    IData> KeyTransformer    { get; }
    public ITransformer<TRecord, IData> RecordTransformer { get; }

    public ITableIndexManager Indexes => Table.Indexes;

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

    public long ScanCount(KeyQuery<TKey> query)
    {
        if (query.Filter is not null)
        {
            long n = 0;
            foreach (var _ in Scan(query)) n++;
            return n;
        }

        if (Table is XTablePortable raw)
        {
            var ifrom = query.HasFrom ? KeyTransformer.To(query.From) : null;
            var ito   = query.HasTo   ? KeyTransformer.To(query.To)   : null;
            return raw.ScanCount(ifrom, query.HasFrom, query.FromExclusive,
                                 ito,  query.HasTo,   query.ToExclusive);
        }

        // Remote fast path: single round trip, server-side leaf-index arithmetic — no records cross the
        // wire. `Table is XTablePortable` above only ever matches the LOCAL in-process table, so without
        // this a remote range count fell straight to full enumeration (a multi-million-row range made a
        // single "count" op take minutes — see AGENTS.md's HighSearch field report).
        if (Table is IRemoteScanTable remote)
        {
            var ifrom = query.HasFrom ? KeyTransformer.To(query.From) : null;
            var ito   = query.HasTo   ? KeyTransformer.To(query.To)   : null;
            return remote.RangeCount(ifrom!, query.HasFrom, query.FromExclusive,
                                      ito!,  query.HasTo,   query.ToExclusive);
        }

        long count = 0;
        foreach (var _ in Scan(query)) count++;
        return count;
    }

    public IEnumerable<KeyValuePair<TKey, TRecord>> Scan(KeyQuery<TKey> query)
    {
        var ifrom  = query.HasFrom ? KeyTransformer.To(query.From) : null;
        var ito    = query.HasTo   ? KeyTransformer.To(query.To)   : null;
        var filter = query.Filter;
        var useSegmentPath = filter is not null || (query.HasFrom && query.HasTo);

        if (Table is XTablePortable raw && useSegmentPath)
        {
            foreach (var seg in raw.ScanSegments(
                         ifrom, query.HasFrom, query.FromExclusive,
                         ito,   query.HasTo,   query.ToExclusive))
            {
                var buf = seg.Buffer;
                var cnt = seg.Count;
                for (var i = 0; i < cnt; i++)
                {
                    var key = KeyTransformer.From(buf[i].Key);
                    if (filter is not null && !filter(key)) continue;
                    yield return new KeyValuePair<TKey, TRecord>(key, RecordTransformer.From(buf[i].Value));
                }
            }
            yield break;
        }

        var eq = System.Collections.Generic.EqualityComparer<TKey>.Default;
        bool skipFirst = query.FromExclusive && query.HasFrom;
        foreach (var kv in Table.Forward(ifrom, query.HasFrom, ito, query.HasTo))
        {
            var key = KeyTransformer.From(kv.Key);
            if (skipFirst) { skipFirst = false; if (eq.Equals(key, query.From)) continue; }
            if (query.ToExclusive && query.HasTo && eq.Equals(key, query.To)) break;
            if (filter is not null && !filter(key)) continue;
            yield return new(key, RecordTransformer.From(kv.Value));
        }
    }

    public IEnumerable<KeyValuePair<TKey, TRecord>> ScanTake(KeyQuery<TKey> query, int take)
    {
        if (take <= 0)
            yield break;

        var ifrom  = query.HasFrom ? KeyTransformer.To(query.From) : null;
        var ito    = query.HasTo   ? KeyTransformer.To(query.To)   : null;
        var filter = query.Filter;
        var useSegmentPath = filter is not null || (query.HasFrom && query.HasTo);

        if (Table is XTablePortable raw && useSegmentPath)
        {
            var produced = 0;
            foreach (var seg in raw.ScanSegments(
                         ifrom, query.HasFrom, query.FromExclusive,
                         ito,   query.HasTo,   query.ToExclusive,
                         take))
            {
                var buf = seg.Buffer;
                var cnt = seg.Count;
                for (var i = 0; i < cnt; i++)
                {
                    var key = KeyTransformer.From(buf[i].Key);
                    if (filter is not null && !filter(key)) continue;
                    yield return new KeyValuePair<TKey, TRecord>(key, RecordTransformer.From(buf[i].Value));
                    if (++produced >= take)
                        yield break;
                }
            }
            yield break;
        }

        // Remote fast path: push the exact row limit to the server (single round-trip, no over-fetch).
        // Only the simple forward range with no upper bound AND NO FILTER goes here — the row-limit
        // pushdown assumes 1 server row = 1 result, which breaks the moment a client-side predicate is
        // attached (the server can't evaluate it, so `take` unfiltered rows can yield fewer, or the wrong,
        // results after filtering). `Table is XTablePortable` above only ever matches the LOCAL in-process
        // table, never XTableRemote, so a filtered query with no upper bound (e.g. AtLeast(x).WithFilter(f))
        // fell all the way through to here and was returned completely unfiltered — every row from the
        // server, filter silently skipped. Excluding `filter is not null` sends it to the Scan() fallback
        // below instead, which does apply the filter correctly.
        if (Table is IRemoteScanTable remote && !query.HasTo && filter is null)
        {
            // +1 covers the exclusive-from skip (the `from` key itself is dropped below).
            var want = take + (query.FromExclusive && query.HasFrom ? 1 : 0);
            var eqr = System.Collections.Generic.EqualityComparer<TKey>.Default;
            var skipFirst = query.FromExclusive && query.HasFrom;
            var emitted = 0;
            foreach (var kv in remote.ForwardTake(ifrom, query.HasFrom, null, false, want))
            {
                var key = KeyTransformer.From(kv.Key);
                if (skipFirst) { skipFirst = false; if (eqr.Equals(key, query.From)) continue; }
                yield return new KeyValuePair<TKey, TRecord>(key, RecordTransformer.From(kv.Value));
                if (++emitted >= take)
                    yield break;
            }
            yield break;
        }

        var n = 0;
        foreach (var kv in Scan(query))
        {
            yield return kv;
            if (++n >= take)
                yield break;
        }
    }

    public IEnumerable<KeyValuePair<TKey, TRecord>> ScanBackward(KeyQuery<TKey> query)
    {
        var ifrom  = query.HasFrom ? KeyTransformer.To(query.From) : null;
        var ito    = query.HasTo   ? KeyTransformer.To(query.To)   : null;
        var filter = query.Filter;
        var useSegmentPath = filter is not null || (query.HasFrom && query.HasTo);

        if (Table is XTablePortable raw && useSegmentPath)
        {
            foreach (var seg in raw.ScanSegmentsBackward(
                         ito,   query.HasTo,   query.ToExclusive,
                         ifrom, query.HasFrom, query.FromExclusive))
            {
                var buf = seg.Buffer;
                var cnt = seg.Count;
                for (var i = 0; i < cnt; i++)
                {
                    var key = KeyTransformer.From(buf[i].Key);
                    if (filter is not null && !filter(key)) continue;
                    yield return new KeyValuePair<TKey, TRecord>(key, RecordTransformer.From(buf[i].Value));
                }
            }
            yield break;
        }

        var eq = System.Collections.Generic.EqualityComparer<TKey>.Default;
        bool skipFirst = query.ToExclusive && query.HasTo;
        foreach (var kv in Table.Backward(ito, query.HasTo, ifrom, query.HasFrom))
        {
            var key = KeyTransformer.From(kv.Key);
            if (skipFirst) { skipFirst = false; if (eq.Equals(key, query.To)) continue; }
            if (query.FromExclusive && query.HasFrom && eq.Equals(key, query.From)) break;
            if (filter is not null && !filter(key)) continue;
            yield return new(key, RecordTransformer.From(kv.Value));
        }
    }

    public IEnumerable<KeyValuePair<TKey, TRecord>> ScanBackwardTake(KeyQuery<TKey> query, int take)
    {
        if (take <= 0)
            yield break;

        var ifrom  = query.HasFrom ? KeyTransformer.To(query.From) : null;
        var ito    = query.HasTo   ? KeyTransformer.To(query.To)   : null;
        var filter = query.Filter;
        var useSegmentPath = filter is not null || (query.HasFrom && query.HasTo);

        if (Table is XTablePortable raw && useSegmentPath)
        {
            var produced = 0;
            foreach (var seg in raw.ScanSegmentsBackward(
                         ito,   query.HasTo,   query.ToExclusive,
                         ifrom, query.HasFrom, query.FromExclusive,
                         take))
            {
                var buf = seg.Buffer;
                var cnt = seg.Count;
                for (var i = 0; i < cnt; i++)
                {
                    var key = KeyTransformer.From(buf[i].Key);
                    if (filter is not null && !filter(key)) continue;
                    yield return new KeyValuePair<TKey, TRecord>(key, RecordTransformer.From(buf[i].Value));
                    if (++produced >= take)
                        yield break;
                }
            }
            yield break;
        }

        // Remote fast path: push the row limit to the server (single round-trip). Simple backward
        // range with no lower bound AND NO FILTER — see the matching comment in ScanTake above for why
        // `filter is null` is required (the row-limit pushdown can't account for client-side filtering,
        // silently returning unfiltered rows for e.g. AtMost(x).WithFilter(f) otherwise).
        if (Table is IRemoteScanTable remote && !query.HasFrom && filter is null)
        {
            var want = take + (query.ToExclusive && query.HasTo ? 1 : 0);
            var eqr = System.Collections.Generic.EqualityComparer<TKey>.Default;
            var skipFirst = query.ToExclusive && query.HasTo;
            var emitted = 0;
            foreach (var kv in remote.BackwardTake(ito, query.HasTo, null, false, want))
            {
                var key = KeyTransformer.From(kv.Key);
                if (skipFirst) { skipFirst = false; if (eqr.Equals(key, query.To)) continue; }
                yield return new KeyValuePair<TKey, TRecord>(key, RecordTransformer.From(kv.Value));
                if (++emitted >= take)
                    yield break;
            }
            yield break;
        }

        var n = 0;
        foreach (var kv in ScanBackward(query))
        {
            yield return kv;
            if (++n >= take)
                yield break;
        }
    }

    public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator() => Forward().GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator()                         => GetEnumerator();
}
