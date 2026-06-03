// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using System.Collections;
using CatDb.Data;
using CatDb.Database.Indexing;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public class XTable<TKey, TRecord>(ITable<IData, IData> table) : ITable<TKey, TRecord>
{
    public ITable<IData, IData> Table { get; } = table ?? throw new ArgumentNullException(nameof(table));

    public ITableIndexManager Indexes => Table.Indexes;

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

    public bool TryGet(TKey key, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TRecord? record)
    {
        if (!Table.TryGet(K(key), out var irec))
        {
            record = default;
            return false;
        }
        record = FromR(irec)!;
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

    public KeyValuePair<TKey, TRecord>? FirstRow => Nullable(Table.FirstRow);
    public KeyValuePair<TKey, TRecord>? LastRow  => Nullable(Table.LastRow);

    /// <summary>
    /// Counts matching records using engine-native index arithmetic when possible.
    /// For sorted-list-mode leaves: O(log leafSize) per leaf — no record access at all.
    /// For 2M records across ~62 leaves: ~62 binary searches instead of 2M IData casts.
    /// When no <see cref="KeyQuery{TKey}.Filter"/> is set, no record data is touched.
    /// </summary>
    public long ScanCount(KeyQuery<TKey> query)
    {
        if (query.Filter is not null)
        {
            // Filter requires per-record evaluation — must iterate
            long n = 0;
            foreach (var _ in Scan(query)) n++;
            return n;
        }

        if (Table is XTablePortable raw)
        {
            var ifrom = query.HasFrom ? K(query.From) : null;
            var ito   = query.HasTo   ? K(query.To)   : null;
            return raw.ScanCount(ifrom, query.HasFrom, query.FromExclusive,
                                 ito,  query.HasTo,   query.ToExclusive);
        }

        // Fallback for remote tables
        long count = 0;
        foreach (var _ in Scan(query)) count++;
        return count;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// When the underlying table is a local <see cref="XTablePortable"/> the call uses
    /// <c>ScanSegments</c> which yields one buffer per <b>leaf</b> (not per record).
    /// This tight <c>for(i)</c> loop with direct <c>buffer[i]</c> access eliminates
    /// 2 of the 3 iterator state machine transitions that the old approach required.
    /// The leaf lock is held only during the buffer copy, not during caller processing.
    /// For remote tables the default interface fallback is used.
    /// </remarks>
    public IEnumerable<KeyValuePair<TKey, TRecord>> Scan(KeyQuery<TKey> query)
    {
        var ifrom  = query.HasFrom ? K(query.From) : null;
        var ito    = query.HasTo   ? K(query.To)   : null;
        var filter = query.Filter;
        var useSegmentPath = filter is not null || (query.HasFrom && query.HasTo);

        if (Table is XTablePortable raw && useSegmentPath)
        {
            // ScanSegments yields once per LEAF — the for(i) loop below
            // is the only per-record work, with direct array indexing.
            foreach (var seg in raw.ScanSegments(
                         ifrom, query.HasFrom, query.FromExclusive,
                         ito,   query.HasTo,   query.ToExclusive))
            {
                var buf = seg.Buffer;
                var cnt = seg.Count;
                for (var i = 0; i < cnt; i++)
                {
                    var key = FromK(buf[i].Key);
                    if (filter is not null && !filter(key)) continue;
                    yield return new KeyValuePair<TKey, TRecord>(key, FromR(buf[i].Value));
                }
            }
            yield break;
        }

        // Fallback: handles XTableRemote or any other ITable<IData,IData>
        var eq = System.Collections.Generic.EqualityComparer<TKey>.Default;
        bool skipFirst = query.FromExclusive && query.HasFrom;
        foreach (var kv in Table.Forward(ifrom, query.HasFrom, ito, query.HasTo))
        {
            var key = FromK(kv.Key);
            if (skipFirst) { skipFirst = false; if (eq.Equals(key, query.From)) continue; }
            if (query.ToExclusive && query.HasTo && eq.Equals(key, query.To)) break;
            if (filter is not null && !filter(key)) continue;
            yield return new(key, FromR(kv.Value));
        }
    }

    public IEnumerable<KeyValuePair<TKey, TRecord>> ScanTake(KeyQuery<TKey> query, int take)
    {
        if (take <= 0)
            yield break;

        var ifrom  = query.HasFrom ? K(query.From) : null;
        var ito    = query.HasTo   ? K(query.To)   : null;
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
                    var key = FromK(buf[i].Key);
                    if (filter is not null && !filter(key)) continue;
                    yield return new KeyValuePair<TKey, TRecord>(key, FromR(buf[i].Value));
                    if (++produced >= take)
                        yield break;
                }
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

    /// <inheritdoc/>
    public IEnumerable<KeyValuePair<TKey, TRecord>> ScanBackward(KeyQuery<TKey> query)
    {
        var ifrom  = query.HasFrom ? K(query.From) : null;
        var ito    = query.HasTo   ? K(query.To)   : null;
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
                    var key = FromK(buf[i].Key);
                    if (filter is not null && !filter(key)) continue;
                    yield return new KeyValuePair<TKey, TRecord>(key, FromR(buf[i].Value));
                }
            }
            yield break;
        }

        var eq = System.Collections.Generic.EqualityComparer<TKey>.Default;
        bool skipFirst = query.ToExclusive && query.HasTo;
        foreach (var kv in Table.Backward(ito, query.HasTo, ifrom, query.HasFrom))
        {
            var key = FromK(kv.Key);
            if (skipFirst) { skipFirst = false; if (eq.Equals(key, query.To)) continue; }
            if (query.FromExclusive && query.HasFrom && eq.Equals(key, query.From)) break;
            if (filter is not null && !filter(key)) continue;
            yield return new(key, FromR(kv.Value));
        }
    }

    public IEnumerable<KeyValuePair<TKey, TRecord>> ScanBackwardTake(KeyQuery<TKey> query, int take)
    {
        if (take <= 0)
            yield break;

        var ifrom  = query.HasFrom ? K(query.From) : null;
        var ito    = query.HasTo   ? K(query.To)   : null;
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
                    var key = FromK(buf[i].Key);
                    if (filter is not null && !filter(key)) continue;
                    yield return new KeyValuePair<TKey, TRecord>(key, FromR(buf[i].Value));
                    if (++produced >= take)
                        yield break;
                }
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
