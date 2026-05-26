#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
using System.Collections.Concurrent;
using CatDb.WaterfallTree;
using SysKvp = System.Collections.Generic.KeyValuePair<long, byte[]>;

namespace CatDb.Storage;

/// <summary>
/// A WAL (Write-Ahead Log) wrapper around Heap that provides crash-safe commits.
///
/// Protocol (optimised for throughput):
///   1. Write() calls are buffered in memory ONLY (no disk I/O until commit).
///   2. Commit() writes ALL buffered data to WAL in one sequential burst, fsyncs,
///      then checkpoints to the main heap and deletes the WAL.
///   3. On open: valid WAL with COMMIT → replay into heap. Incomplete WAL → discard.
///
/// Thread-safety:
///   - Read/Write/Exists are LOCK-FREE (ConcurrentDictionary).
///   - Commit serializes only with other Commits (not with reads).
///   - Correctness relies on: checkpoint to heap BEFORE clear from pendingWrites.
///     Any racing Read either finds data in pendingWrites (not yet cleared) or in
///     the heap (already checkpointed).
/// </summary>
public sealed class WalHeap : IHeap
{
    private readonly Heap _heap;
    private readonly string _walPath;
    private readonly object _commitLock = new();

    // Buffered writes: ConcurrentDictionary for lock-free Read/Write/Exists
    private readonly ConcurrentDictionary<long, byte[]> _pendingWrites = new();

    // WAL format constants
    private static readonly byte[] WAL_MAGIC = "CATWAL01"u8.ToArray();
    private const byte RECORD_WRITE = 0x01;
    private const byte RECORD_COMMIT = 0x02;
    private const int CHECKSUM_SEED = unchecked((int)0xDEADBEEF);

    public WalHeap(Heap heap, string walPath)
    {
        _heap = heap ?? throw new ArgumentNullException(nameof(heap));
        _walPath = walPath ?? throw new ArgumentNullException(nameof(walPath));

        Recover();
    }

    #region IHeap — hot path (lock-free)

    public long ObtainNewHandle() => _heap.ObtainNewHandle();

    public void Release(long handle)
    {
        // Remove from pending FIRST — prevents checkpointing a released handle
        _pendingWrites.TryRemove(handle, out _);
        _heap.Release(handle);
    }

    public bool Exists(long handle)
    {
        // Check pending first (catches uncommitted writes), then heap
        return _pendingWrites.ContainsKey(handle) || _heap.Exists(handle);
    }

    public byte[] Read(long handle)
    {
        // Lock-free: check pending first, fall through to heap
        if (_pendingWrites.TryGetValue(handle, out var buffer))
            return buffer;
        return _heap.Read(handle);
    }

    public byte[] Tag
    {
        get => _heap.Tag;
        set => _heap.Tag = value;
    }

    public long DataSize => _heap.DataSize;
    public long Size => _heap.Size;

    #endregion

    #region Write — lock-free memory buffer only

    public void Write(long handle, byte[] buffer, int index, int count)
    {
        byte[] buf;
        if (index == 0 && count == buffer.Length)
            buf = buffer;
        else
        {
            buf = new byte[count];
            Buffer.BlockCopy(buffer, index, buf, 0, count);
        }

        _pendingWrites[handle] = buf;
    }

    #endregion

    #region Commit — WAL burst write then checkpoint (does NOT block reads)

    public void Commit()
    {
        lock (_commitLock)
        {
            if (_pendingWrites.IsEmpty)
            {
                _heap.Commit();
                return;
            }

            // Snapshot current pending writes
            var snapshot = _pendingWrites.ToArray<SysKvp>();

            // 1. Write WAL (readers continue unblocked — they read from _pendingWrites or heap)
            WriteWalBurst(snapshot);

            // 2. Checkpoint: write all entries to real heap
            //    After each _heap.Write(), the data is readable from the heap.
            foreach (var kv in snapshot)
                _heap.Write(kv.Key, kv.Value, 0, kv.Value.Length);
            _heap.Commit();

            // 3. Clear pending entries (AFTER checkpoint — any racing Read will find data in heap)
            foreach (var kv in snapshot)
                _pendingWrites.TryRemove(kv.Key, out _);

            // 4. Remove WAL
            DeleteWalFile();
        }
    }

    private void WriteWalBurst(SysKvp[] snapshot)
    {
        using var fs = new FileStream(_walPath, FileMode.Create, FileAccess.Write,
            FileShare.None, 65536, FileOptions.SequentialScan);
        using var writer = new BinaryWriter(fs);

        writer.Write(WAL_MAGIC);

        foreach (var kv in snapshot)
        {
            writer.Write(RECORD_WRITE);
            writer.Write(kv.Key);
            writer.Write(kv.Value.Length);
            writer.Write(kv.Value);
            writer.Write(ComputeChecksum(kv.Value, 0, kv.Value.Length, kv.Key));
        }

        writer.Write(RECORD_COMMIT);
        writer.Write(snapshot.Length);
        writer.Write(ComputeCommitChecksum(snapshot));

        fs.Flush(flushToDisk: true);
    }

    #endregion

    #region Close

    public void Close()
    {
        lock (_commitLock)
        {
            _pendingWrites.Clear();
            _heap.Close();
        }
    }

    #endregion

    #region Recovery

    private void Recover()
    {
        if (!File.Exists(_walPath))
            return;

        var entries = new List<SysKvp>();
        var hasValidCommit = false;

        try
        {
            using var fs = new FileStream(_walPath, FileMode.Open, FileAccess.Read, FileShare.None);
            using var reader = new BinaryReader(fs);

            var magic = reader.ReadBytes(WAL_MAGIC.Length);
            if (!magic.AsSpan().SequenceEqual(WAL_MAGIC))
            {
                fs.Close();
                DeleteWalFile();
                return;
            }

            while (fs.Position < fs.Length)
            {
                byte recordType;
                try { recordType = reader.ReadByte(); }
                catch { break; }

                if (recordType == RECORD_WRITE)
                {
                    long handle;
                    int count;
                    try
                    {
                        handle = reader.ReadInt64();
                        count = reader.ReadInt32();
                    }
                    catch { break; }

                    if (count < 0 || count > 100_000_000) break;

                    byte[] buffer;
                    try { buffer = reader.ReadBytes(count); }
                    catch { break; }
                    if (buffer.Length != count) break;

                    int checksum;
                    try { checksum = reader.ReadInt32(); }
                    catch { break; }

                    if (checksum != ComputeChecksum(buffer, 0, count, handle)) break;

                    entries.Add(new SysKvp(handle, buffer));
                }
                else if (recordType == RECORD_COMMIT)
                {
                    int expectedCount;
                    int commitChecksum;
                    try
                    {
                        expectedCount = reader.ReadInt32();
                        commitChecksum = reader.ReadInt32();
                    }
                    catch { break; }

                    if (expectedCount != entries.Count) break;
                    if (commitChecksum != ComputeCommitChecksum(entries)) break;

                    hasValidCommit = true;
                    break;
                }
                else break;
            }
        }
        catch { hasValidCommit = false; }

        if (hasValidCommit && entries.Count > 0)
        {
            foreach (var entry in entries)
                _heap.Write(entry.Key, entry.Value, 0, entry.Value.Length);
            _heap.Commit();
        }

        DeleteWalFile();
    }

    private void DeleteWalFile()
    {
        try { if (File.Exists(_walPath)) File.Delete(_walPath); }
        catch { }
    }

    #endregion

    #region Checksums

    private static int ComputeChecksum(byte[] buffer, int index, int count, long handle)
    {
        unchecked
        {
            var hash = CHECKSUM_SEED;
            hash ^= (int)(handle & 0xFFFFFFFF);
            hash *= 16777619;
            hash ^= (int)(handle >> 32);
            hash *= 16777619;
            hash ^= count;
            hash *= 16777619;

            for (var i = index; i < index + count; i++)
            {
                hash ^= buffer[i];
                hash *= 16777619;
            }
            return hash;
        }
    }

    private static int ComputeCommitChecksum(SysKvp[] snapshot)
    {
        unchecked
        {
            var hash = CHECKSUM_SEED;
            hash ^= snapshot.Length;
            hash *= 16777619;

            foreach (var kv in snapshot)
            {
                hash ^= (int)(kv.Key & 0xFFFFFFFF);
                hash *= 16777619;
                hash ^= kv.Value.Length;
                hash *= 16777619;
            }
            return hash;
        }
    }

    private static int ComputeCommitChecksum(IList<SysKvp> entries)
    {
        unchecked
        {
            var hash = CHECKSUM_SEED;
            hash ^= entries.Count;
            hash *= 16777619;

            foreach (var entry in entries)
            {
                hash ^= (int)(entry.Key & 0xFFFFFFFF);
                hash *= 16777619;
                hash ^= entry.Value.Length;
                hash *= 16777619;
            }
            return hash;
        }
    }

    #endregion
}
