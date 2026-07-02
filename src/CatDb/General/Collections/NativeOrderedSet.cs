// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Runtime.InteropServices;
using CatDb.General.Persist;
using CatDb.General.Threading;

#pragma warning disable CS8602, CS8604, CS8600, CS8601, CS8603
namespace CatDb.General.Collections;

/// <summary>
/// SQL-Server / Postgres-style <b>native slotted page</b> ordered set. The proven managed
/// <see cref="OrderedSet{TKey,TValue}"/> stores one boxed key, one record object, AND one red-black
/// <c>SortedSet.Node</c> object PER ROW on the GC heap — at tens of millions of rows that is gigabytes
/// of gen2 that the GC must walk and the OS pages out, producing the multi-second freezes.
///
/// This implementation stores ALL row data in <b>unmanaged memory</b> via pointers:
/// <list type="bullet">
///   <item>a native, always-sorted <c>Slot</c> array (binary-searched in place), and</item>
///   <item>a native byte arena holding serialized key/record bytes.</item>
/// </list>
/// ZERO managed objects per row are retained — key/record objects are materialized only transiently
/// at the API boundary (and immediately collectable gen0). For fixed-width primitive keys (the hot
/// path: long/int/DateTime/…) the key is stored INLINE in the slot and compared with pure integer
/// arithmetic — no deserialization, no boxing during binary search.
///
/// Thread-safety: identical to the managed set — callers serialize mutations vs reads via <see cref="Lock"/>
/// (mutations take <c>EnterWrite</c>, reads <c>EnterRead</c>). The exclusive write lock guarantees no reader
/// is materializing from the arena while a writer reallocates/moves it.
/// </summary>
public sealed unsafe class NativeOrderedSet : IOrderedSet<IData, IData>, IDisposable
{
    [StructLayout(LayoutKind.Sequential)]
    private struct Slot
    {
        public long Inline;  // inline key value (when InlineKind != None) — compared directly
        public int  KeyOff;  // arena offset of serialized key bytes (non-inline keys only)
        public int  KeyLen;
        public int  RecOff;  // arena offset of serialized record bytes
        public int  RecLen;
    }

    private enum InlineKind : byte { None = 0, Signed, Unsigned }

    public DataRwLock Lock { get; } = new();

    private readonly IComparer<IData>          _comparer;
    private readonly IEqualityComparer<IData>  _equalityComparer;
    private readonly IPersist<IData>           _keyPersist;
    private readonly IPersist<IData>           _recordPersist;
    private readonly InlineKind                _inlineKind;
    private readonly Type?                     _keyType;

    private Slot* _slots;
    private int   _count;
    private int   _slotCap;

    private byte* _arena;
    private int   _arenaLen;
    private int   _arenaCap;
    private int   _deadBytes;   // arena bytes orphaned by overwrite/delete — triggers compaction
    private long  _pressure;    // native bytes allocated by THIS set (also tracked globally below)
    private int   _disposed;

    // Global total native bytes across all live NativeOrderedSets — read by Dashboard for accurate mem display.
    public static long GlobalNativeBytes;

    // Reusable serialization scratch — one MemoryStream/BinaryWriter PER THREAD, shared across all sets
    // written on that thread (writes hold the leaf's exclusive lock, so no concurrent reuse). Removes the
    // per-op MemoryStream + ToArray() allocation; bytes are then copied straight into the native arena.
    [ThreadStatic] private static MemoryStream? _tlWriteMs;
    [ThreadStatic] private static BinaryWriter? _tlWriteBw;

    // Reusable READ scratch — per thread (reads run concurrently under the shared read lock, so each thread
    // needs its own). A row's bytes are copied here then deserialized, avoiding a per-row stream/reader alloc.
    [ThreadStatic] private static byte[]?       _tlReadBuf;
    [ThreadStatic] private static MemoryStream? _tlReadMs;
    [ThreadStatic] private static BinaryReader? _tlReadBr;

    public NativeOrderedSet(
        IComparer<IData> comparer,
        IEqualityComparer<IData> equalityComparer,
        IPersist<IData> keyPersist,
        IPersist<IData> recordPersist,
        Type? keyType)
    {
        _comparer         = comparer;
        _equalityComparer = equalityComparer;
        _keyPersist       = keyPersist;
        _recordPersist    = recordPersist;
        _keyType          = keyType;
        _inlineKind       = ClassifyInline(keyType);
    }

    private static InlineKind ClassifyInline(Type? t)
    {
        if (t == typeof(long) || t == typeof(int) || t == typeof(short) || t == typeof(sbyte)
            || t == typeof(byte) || t == typeof(ushort) || t == typeof(uint)
            || t == typeof(bool) || t == typeof(char) || t == typeof(DateTime))
            return InlineKind.Signed;       // all fit in a non-negative-preserving signed long compare
        if (t == typeof(ulong))
            return InlineKind.Unsigned;     // needs unsigned comparison
        return InlineKind.None;             // double/float/decimal/string/composite → deserialize to compare
    }

    // ── inline key encode/decode ──────────────────────────────────────────────

    private long EncodeInline(IData key) => key switch
    {
        long l     => l,
        int i      => i,
        short s    => s,
        sbyte sb   => sb,
        byte b     => b,
        ushort us  => us,
        uint ui    => ui,
        bool bo    => bo ? 1L : 0L,
        char c     => c,
        DateTime d => d.Ticks,
        ulong ul   => unchecked((long)ul),
        _          => 0L
    };

    private IData DecodeInline(long v)
    {
        var t = _keyType;
        if (t == typeof(long))     return v;
        if (t == typeof(int))      return (int)v;
        if (t == typeof(short))    return (short)v;
        if (t == typeof(sbyte))    return (sbyte)v;
        if (t == typeof(byte))     return (byte)v;
        if (t == typeof(ushort))   return (ushort)v;
        if (t == typeof(uint))     return (uint)v;
        if (t == typeof(bool))     return v != 0;
        if (t == typeof(char))     return (char)v;
        if (t == typeof(DateTime)) return new DateTime(v);
        if (t == typeof(ulong))    return unchecked((ulong)v);
        return v;
    }

    // ── native memory management ──────────────────────────────────────────────

    private void EnsureSlotCap(int needed)
    {
        if (needed <= _slotCap) return;
        var cap = _slotCap == 0 ? 4 : _slotCap * 2;
        if (cap < needed) cap = needed;
        var bytes = (nuint)((long)cap * sizeof(Slot));
        _slots = (Slot*)NativeMemory.Realloc(_slots, bytes);
        AdjustPressure((long)cap * sizeof(Slot) - (long)_slotCap * sizeof(Slot));
        _slotCap = cap;
    }

    private void EnsureArenaCap(int extra)
    {
        if (_arenaLen + extra <= _arenaCap) return;
        var cap = _arenaCap == 0 ? 64 : _arenaCap * 2;
        while (cap < _arenaLen + extra) cap *= 2;
        _arena = (byte*)NativeMemory.Realloc(_arena, (nuint)cap);
        AdjustPressure(cap - _arenaCap);
        _arenaCap = cap;
    }

    // Append a raw native byte range into the arena (used by Split/compaction — pure pointer copy).
    private int ArenaAppend(byte* src, int len)
    {
        EnsureArenaCap(len);
        var off = _arenaLen;
        if (len > 0) Buffer.MemoryCopy(src, _arena + off, len, len);
        _arenaLen += len;
        return off;
    }

    private void AdjustPressure(long delta)
    {
        if (delta == 0) return;
        _pressure += delta;
        Interlocked.Add(ref GlobalNativeBytes, delta);
        if (delta > 0) GC.AddMemoryPressure(delta);
        else           GC.RemoveMemoryPressure(-delta);
    }

    // ── serialize directly into the native arena (no per-op managed allocation) ─

    // Serializes value via the pooled thread-static writer, then copies the bytes straight into the
    // arena via a pointer. Returns the arena offset; sets len. ZERO heap allocation per op (the scratch
    // MemoryStream is reused across all ops on the thread; its backing array only grows to the max row).
    private int SerializeIntoArena(IData value, IPersist<IData> persist, out int len)
    {
        var ms = _tlWriteMs ??= new MemoryStream(256);
        var bw = _tlWriteBw ??= new BinaryWriter(ms);
        ms.Position = 0;
        persist.Write(bw, value);
        bw.Flush();
        len = (int)ms.Position;

        EnsureArenaCap(len);
        var off = _arenaLen;
        if (len > 0)
            fixed (byte* src = ms.GetBuffer())   // GetBuffer: no copy, direct access to the backing array
                Buffer.MemoryCopy(src, _arena + off, len, len);
        _arenaLen += len;
        return off;
    }

    // ── raw-bytes checkpoint passthrough (no materialize/reserialize/compress) ──
    //
    // The arena already holds each row's serialized key/record bytes. The checkpoint can copy them
    // STRAIGHT to disk instead of deserializing every row back into objects and column-recompressing
    // them (which made a single 555 KB leaf take ~3 s to Store → the global checkpoint freeze). Iterating
    // SLOTS (not the arena) naturally writes only LIVE rows, so this also compacts away dead bytes on disk.

    public void WriteRawTo(BinaryWriter writer)
    {
        Lock.EnterRead();
        try
        {
            writer.Write(_count);
            writer.Write((byte)_inlineKind);
            for (var i = 0; i < _count; i++)
            {
                ref var s = ref _slots[i];
                if (_inlineKind != InlineKind.None)
                    writer.Write(s.Inline);
                else
                {
                    writer.Write(s.KeyLen);
                    if (s.KeyLen > 0) writer.Write(new ReadOnlySpan<byte>(_arena + s.KeyOff, s.KeyLen));
                }
                writer.Write(s.RecLen);
                if (s.RecLen > 0) writer.Write(new ReadOnlySpan<byte>(_arena + s.RecOff, s.RecLen));
            }
        }
        finally { Lock.ExitRead(); }
    }

    public void ReadRawFrom(BinaryReader reader)
    {
        _count = 0;
        ResetArena();

        var count = reader.ReadInt32();
        var kind  = (InlineKind)reader.ReadByte();
        if (kind != _inlineKind)
            throw new InvalidDataException("NativeOrderedSet inline-kind mismatch on load.");

        EnsureSlotCap(count);
        for (var i = 0; i < count; i++)
        {
            var slot = new Slot();
            if (kind != InlineKind.None)
                slot.Inline = reader.ReadInt64();
            else
            {
                var klen = reader.ReadInt32();
                slot.KeyOff = ArenaReadFrom(reader, klen);
                slot.KeyLen = klen;
            }
            var rlen = reader.ReadInt32();
            slot.RecOff = ArenaReadFrom(reader, rlen);
            slot.RecLen = rlen;
            _slots[i] = slot;
        }
        _count = count;
    }

    // Read `len` bytes from the stream STRAIGHT into the native arena (no managed intermediate buffer).
    private int ArenaReadFrom(BinaryReader reader, int len)
    {
        EnsureArenaCap(len);
        var off = _arenaLen;
        if (len > 0)
        {
            var span = new Span<byte>(_arena + off, len);
            var read = 0;
            while (read < len)
            {
                var n = reader.Read(span.Slice(read));
                if (n <= 0) throw new EndOfStreamException();
                read += n;
            }
        }
        _arenaLen += len;
        return off;
    }

    // Rebuild the arena keeping only live rows' bytes — reclaims the space orphaned by overwrite/delete.
    // Called under the exclusive write lock (mutations only), so no reader can be materializing meanwhile.
    private void Compact()
    {
        if (_count == 0) { _arenaLen = 0; _deadBytes = 0; return; }

        long live = 0;
        for (var i = 0; i < _count; i++)
        {
            if (_inlineKind == InlineKind.None) live += _slots[i].KeyLen;
            live += _slots[i].RecLen;
        }

        var newCap = (int)Math.Max(live, 64);
        var na = (byte*)NativeMemory.Alloc((nuint)newCap);
        var pos = 0;
        for (var i = 0; i < _count; i++)
        {
            ref var s = ref _slots[i];
            if (_inlineKind == InlineKind.None && s.KeyLen > 0)
            {
                Buffer.MemoryCopy(_arena + s.KeyOff, na + pos, s.KeyLen, s.KeyLen);
                s.KeyOff = pos; pos += s.KeyLen;
            }
            if (s.RecLen > 0)
            {
                Buffer.MemoryCopy(_arena + s.RecOff, na + pos, s.RecLen, s.RecLen);
                s.RecOff = pos; pos += s.RecLen;
            }
        }

        NativeMemory.Free(_arena);
        AdjustPressure((long)newCap - _arenaCap);
        _arena = na;
        _arenaCap = newCap;
        _arenaLen = pos;
        _deadBytes = 0;
    }

    // Compact when the dead space is both absolutely and relatively significant (avoids churn on small sets).
    private void MaybeCompact()
    {
        if (_deadBytes > 65536 && (long)_deadBytes * 2 >= _arenaLen)
            Compact();
    }

    private IData ReadKey(in Slot s)
    {
        if (_inlineKind != InlineKind.None)
            return DecodeInline(s.Inline);
        return ReadFromArena(s.KeyOff, s.KeyLen, _keyPersist);
    }

    private IData ReadRecord(in Slot s)
        => ReadFromArena(s.RecOff, s.RecLen, _recordPersist);

    // Deserialize one value from the arena WITHOUT allocating a stream/reader per row. Earlier this did
    // `new UnmanagedMemoryStream + new BinaryReader` PER ROW — at scan rates of millions of rows that was
    // 5-10 GB/20s of gen0 churn → frequent multi-second GC pauses that, hitting under the checkpoint root
    // lock, froze every reader+writer. Now: copy the bytes into a reused thread-static buffer (each thread
    // its own — reads run concurrently under the shared lock) and read from a reused MemoryStream/BinaryReader.
    private IData ReadFromArena(int off, int len, IPersist<IData> persist)
    {
        var buf = _tlReadBuf;
        if (buf == null || buf.Length < len)
        {
            buf       = new byte[Math.Max(len, 256)];
            _tlReadBuf = buf;
            _tlReadMs  = new MemoryStream(buf, 0, buf.Length, writable: true, publiclyVisible: true);
            _tlReadBr  = new BinaryReader(_tlReadMs);
        }
        if (len > 0)
            fixed (byte* dst = buf)
                Buffer.MemoryCopy(_arena + off, dst, buf.Length, len);
        _tlReadMs!.Position = 0;
        return persist.Read(_tlReadBr!);
    }

    // Materialize one row. MUST be a non-iterator method: iterator state machines cannot deref
    // pointers (_slots) across yield boundaries, so callers yield RowAt(i) instead of inlining.
    private KeyValuePair<IData, IData> RowAt(int i)
        => new(ReadKey(_slots[i]), ReadRecord(_slots[i]));

    // ── comparison ────────────────────────────────────────────────────────────

    // Returns sign of (slot[i].key − searchKey). searchInline is valid only when InlineKind != None.
    private int CompareSlot(int i, IData searchKey, long searchInline)
    {
        switch (_inlineKind)
        {
            case InlineKind.Signed:
                return _slots[i].Inline.CompareTo(searchInline);
            case InlineKind.Unsigned:
                return ((ulong)_slots[i].Inline).CompareTo((ulong)searchInline);
            default:
                return _comparer.Compare(ReadKey(_slots[i]), searchKey);
        }
    }

    // Lower bound: first index whose key >= searchKey.
    private int LowerBound(IData searchKey, long searchInline)
    {
        int lo = 0, hi = _count;
        while (lo < hi)
        {
            var mid = (int)(((uint)lo + (uint)hi) >> 1);
            if (CompareSlot(mid, searchKey, searchInline) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private bool Find(IData key, out int index)
    {
        var inline = _inlineKind != InlineKind.None ? EncodeInline(key) : 0L;
        var lb = LowerBound(key, inline);
        if (lb < _count && CompareSlot(lb, key, inline) == 0)
        {
            index = lb;
            return true;
        }
        index = lb;
        return false;
    }

    // ── insertion ─────────────────────────────────────────────────────────────

    private void InsertAt(int index, IData key, IData record)
    {
        EnsureSlotCap(_count + 1);
        if (index < _count)
            Buffer.MemoryCopy(_slots + index, _slots + index + 1,
                (long)(_slotCap - index - 1) * sizeof(Slot),
                (long)(_count - index) * sizeof(Slot));

        var slot = new Slot();
        if (_inlineKind != InlineKind.None)
        {
            slot.Inline = EncodeInline(key);
            slot.KeyOff = slot.KeyLen = 0;
        }
        else
        {
            slot.KeyOff = SerializeIntoArena(key, _keyPersist, out var klen);
            slot.KeyLen = klen;
        }
        slot.RecOff = SerializeIntoArena(record, _recordPersist, out var rlen);
        slot.RecLen = rlen;

        _slots[index] = slot;
        _count++;
    }

    private void OverwriteRecord(int index, IData record)
    {
        _deadBytes += _slots[index].RecLen;                 // old record bytes orphaned in the arena
        _slots[index].RecOff = SerializeIntoArena(record, _recordPersist, out var rlen);
        _slots[index].RecLen = rlen;
        MaybeCompact();
    }

    // ── IOrderedSet ───────────────────────────────────────────────────────────

    public IComparer<IData>         Comparer         => _comparer;
    public IEqualityComparer<IData> EqualityComparer => _equalityComparer;

    public void Add(IData key, IData value)
    {
        if (Find(key, out var idx))
            OverwriteRecord(idx, value);
        else
            InsertAt(idx, key, value);
    }

    public void Add(KeyValuePair<IData, IData> kv) => Add(kv.Key, kv.Value);

    // Caller asserts monotone-ascending append; defensively fall back to sorted Add on violation.
    public void UnsafeAdd(IData key, IData value)
    {
        if (_count > 0 && CompareSlot(_count - 1, key, _inlineKind != InlineKind.None ? EncodeInline(key) : 0L) >= 0)
        {
            Add(key, value);
            return;
        }
        InsertAt(_count, key, value);
    }

    public bool Remove(IData key)
    {
        if (!Find(key, out var idx)) return false;
        RemoveSlotRange(idx, idx);
        return true;
    }

    public bool Remove(IData from, bool hasFrom, IData to, bool hasTo)
    {
        if (_count == 0) return false;
        if (!hasFrom && !hasTo) { Clear(); return true; }

        var fromInline = _inlineKind != InlineKind.None && hasFrom ? EncodeInline(from) : 0L;
        var toInline   = _inlineKind != InlineKind.None && hasTo   ? EncodeInline(to)   : 0L;

        var start = hasFrom ? LowerBound(from, fromInline) : 0;
        // upper bound for inclusive 'to' = first index whose key > to
        int end;
        if (hasTo)
        {
            var ub = LowerBound(to, toInline);
            while (ub < _count && CompareSlot(ub, to, toInline) == 0) ub++;
            end = ub - 1;
        }
        else end = _count - 1;

        if (start > end) return false;
        RemoveSlotRange(start, end);
        return true;
    }

    private void RemoveSlotRange(int start, int end)
    {
        for (var i = start; i <= end; i++)             // their arena bytes become dead
            _deadBytes += _slots[i].KeyLen + _slots[i].RecLen;

        var removed = end - start + 1;
        var tail = _count - end - 1;
        if (tail > 0)
            Buffer.MemoryCopy(_slots + end + 1, _slots + start,
                (long)(_slotCap - start) * sizeof(Slot),
                (long)tail * sizeof(Slot));
        _count -= removed;
        if (_count == 0) ResetArena();
        else MaybeCompact();
    }

    private void ResetArena()
    {
        // all rows gone — reclaim the arena (slots stay allocated, cheap)
        _arenaLen = 0;
        _deadBytes = 0;
    }

    public bool ContainsKey(IData key) => Find(key, out _);

    public bool TryGetValue(IData key, out IData value)
    {
        if (Find(key, out var idx))
        {
            value = ReadRecord(_slots[idx]);
            return true;
        }
        value = null!;
        return false;
    }

    public IData this[IData key]
    {
        get => TryGetValue(key, out var v) ? v : throw new KeyNotFoundException("The key was not found.");
        set => Add(key, value: value);
    }

    public void Clear()
    {
        _count = 0;
        ResetArena();
    }

    /// <summary>Total native bytes this set has allocated (slots + arena capacity). Exact, not an estimate —
    /// used by the WTree byte-budget eviction, which otherwise only sees the (tiny) managed footprint and
    /// would let native arenas accumulate unbounded.</summary>
    public long AllocatedBytes => (long)_slotCap * sizeof(Slot) + _arenaCap;

    /// <summary>Reclaims over-allocated native capacity: compacts dead bytes and shrinks the arena to the
    /// live size when the arena is badly over-allocated (capacity kept doubling to a high-water mark the
    /// live data no longer needs). Called from LeafNode.Store once the leaf is settled. Takes the exclusive
    /// write lock — Compact moves the arena, and concurrent readers deref arena pointers under EnterRead.</summary>
    public void TrimExcess()
    {
        if (_arenaCap <= 128 * 1024) return;                      // small arenas aren't worth a realloc
        long live = _arenaLen - _deadBytes;
        if (live * 2 >= _arenaCap) return;                        // not badly over-allocated

        Lock.EnterWrite();
        try { Compact(); }
        finally { Lock.ExitWrite(); }
    }

    public bool IsInternallyOrdered => true;   // native set is always sorted

    public IEnumerable<KeyValuePair<IData, IData>> InternalEnumerate()
    {
        for (var i = 0; i < _count; i++)
            yield return RowAt(i);
    }

    public IOrderedSet<IData, IData> Split(int count)
    {
        // Returns the UPPER `count` rows as a new set; keeps the lower (_count − count) here.
        var keep  = _count - count;
        var right = new NativeOrderedSet(_comparer, _equalityComparer, _keyPersist, _recordPersist, _keyType);
        right.EnsureSlotCap(count);

        var movedBytes = 0;
        for (var i = 0; i < count; i++)
        {
            ref var s = ref _slots[keep + i];
            var slot = new Slot { Inline = s.Inline };
            if (_inlineKind == InlineKind.None && s.KeyLen > 0)
            {
                slot.KeyOff = right.ArenaAppend(_arena + s.KeyOff, s.KeyLen);
                slot.KeyLen = s.KeyLen;
                movedBytes += s.KeyLen;
            }
            slot.RecOff = right.ArenaAppend(_arena + s.RecOff, s.RecLen);
            slot.RecLen = s.RecLen;
            movedBytes += s.RecLen;
            right._slots[i] = slot;
        }
        right._count = count;

        _count = keep;
        if (_count == 0) ResetArena();
        else { _deadBytes += movedBytes; MaybeCompact(); }  // upper rows' bytes now dead in this arena
        return right;
    }

    public void Merge(IOrderedSet<IData, IData> set)
    {
        if (set.Count == 0) return;
        // Disjoint contiguous range (append or prepend); Add keeps the array sorted regardless.
        foreach (var kv in set.InternalEnumerate())
            Add(kv.Key, kv.Value);
    }

    public void LoadFrom(KeyValuePair<IData, IData>[] array, int count, bool isOrdered)
    {
        _count = 0;
        ResetArena();
        if (count == 0) return;

        EnsureSlotCap(count);
        if (isOrdered)
        {
            for (var i = 0; i < count; i++)
                InsertAt(_count, array[i].Key, array[i].Value);
        }
        else
        {
            for (var i = 0; i < count; i++)
                Add(array[i].Key, array[i].Value);
        }
    }

    public IEnumerable<KeyValuePair<IData, IData>> Forward(IData from, bool hasFrom, IData to, bool hasTo)
        => ForwardExclusive(from, hasFrom, false, to, hasTo, false);

    public IEnumerable<KeyValuePair<IData, IData>> ForwardExclusive(
        IData from, bool hasFrom, bool fromExclusive,
        IData to,   bool hasTo,   bool toExclusive)
    {
        if (!ComputeRange(from, hasFrom, fromExclusive, to, hasTo, toExclusive, out var start, out var end))
            yield break;
        for (var i = start; i <= end; i++)
            yield return RowAt(i);
    }

    public IEnumerable<KeyValuePair<IData, IData>> Backward(IData to, bool hasTo, IData from, bool hasFrom)
        => BackwardExclusive(to, hasTo, false, from, hasFrom, false);

    public IEnumerable<KeyValuePair<IData, IData>> BackwardExclusive(
        IData to,   bool hasTo,   bool toExclusive,
        IData from, bool hasFrom, bool fromExclusive)
    {
        if (!ComputeRange(from, hasFrom, fromExclusive, to, hasTo, toExclusive, out var start, out var end))
            yield break;
        for (var i = end; i >= start; i--)
            yield return RowAt(i);
    }

    // Resolves an inclusive/exclusive [from,to] window to slot indices [start,end] (both inclusive).
    private bool ComputeRange(
        IData from, bool hasFrom, bool fromExclusive,
        IData to,   bool hasTo,   bool toExclusive,
        out int start, out int end)
    {
        start = 0; end = _count - 1;
        if (_count == 0) return false;

        var fromInline = _inlineKind != InlineKind.None && hasFrom ? EncodeInline(from) : 0L;
        var toInline   = _inlineKind != InlineKind.None && hasTo   ? EncodeInline(to)   : 0L;

        if (hasFrom)
        {
            start = LowerBound(from, fromInline);
            if (fromExclusive)
                while (start < _count && CompareSlot(start, from, fromInline) == 0) start++;
        }
        if (hasTo)
        {
            var ub = LowerBound(to, toInline);
            if (toExclusive)
                end = ub - 1;                       // first key >= to, exclusive → stop before it
            else
            {
                while (ub < _count && CompareSlot(ub, to, toInline) == 0) ub++;
                end = ub - 1;                       // include all keys == to
            }
        }
        return start <= end && start >= 0 && end < _count;
    }

    public IEnumerator<KeyValuePair<IData, IData>> GetEnumerator()
    {
        for (var i = 0; i < _count; i++)
            yield return RowAt(i);
    }

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

    public KeyValuePair<IData, IData> First
        => _count == 0 ? throw new InvalidOperationException("The set is empty.")
                       : new KeyValuePair<IData, IData>(ReadKey(_slots[0]), ReadRecord(_slots[0]));

    public KeyValuePair<IData, IData> Last
        => _count == 0 ? throw new InvalidOperationException("The set is empty.")
                       : new KeyValuePair<IData, IData>(ReadKey(_slots[_count - 1]), ReadRecord(_slots[_count - 1]));

    public int Count => _count;

    // ── native lifetime ───────────────────────────────────────────────────────

    /// <summary>Deterministic reclaim. Takes the exclusive write lock so a late reader still holding this
    /// set (page scans keep the reference briefly after the branch lock is released) can never observe a
    /// freed arena — it either finishes under its read lock first, or acquires after and sees an empty set.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        Lock.EnterWrite();
        try { FreeNative(); }
        finally { Lock.ExitWrite(); }
        GC.SuppressFinalize(this);
    }

    private void FreeNative()
    {
        if (_slots != null) { NativeMemory.Free(_slots); _slots = null; }
        if (_arena != null) { NativeMemory.Free(_arena); _arena = null; }
        if (_pressure > 0)
        {
            GC.RemoveMemoryPressure(_pressure);
            Interlocked.Add(ref GlobalNativeBytes, -_pressure);   // keep the global gauge honest
            _pressure = 0;
        }
        _count = _slotCap = _arenaLen = _arenaCap = 0;
        _deadBytes = 0;
    }

    ~NativeOrderedSet()
    {
        // Safety net for any drop site that misses the deterministic reclaim path. No lock from the
        // finalizer thread — by definition nothing references the set anymore.
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        FreeNative();
    }
}
