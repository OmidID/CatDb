// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.General.Threading;

namespace CatDb.Storage;

/// <summary>
/// Append-only logical redo log for <see cref="CommitMode.TransactionLog"/>. Each record is one applied
/// operation batch tagged with a monotonic LSN; a commit writes a COMMIT marker and fsyncs, which is what
/// makes a transaction durable (cheap sequential append instead of serialising dirty nodes). A background
/// checkpoint persists the nodes to the heap and then <see cref="Truncate"/>s the log.
///
/// Format mirrors the proven <see cref="WalHeap"/> recovery pattern: magic header, length-prefixed records,
/// per-record FNV checksum, explicit COMMIT markers, replay-up-to-last-valid-COMMIT then discard the torn
/// tail. The payload is opaque here — the WTree supplies the writer/reader (it serialises a locator id +
/// the operation batch), so this class has no dependency on the tree or operation model.
/// </summary>
public sealed class OperationLog : IDisposable
{
    private static readonly byte[] Magic = "CATOPLOG1"u8.ToArray();
    private const byte RecordBatch = 0x01;
    private const byte RecordCommit = 0x02;
    private const int ChecksumSeed = unchecked((int)0xCA7D10F0);

    private readonly string _path;
    private readonly ReentrantLock _sync = new();
    private FileStream _stream;
    private BinaryWriter _writer;

    public OperationLog(string path)
    {
        _path = path;
        _stream = OpenAppend(path, out var isNew);
        _writer = new BinaryWriter(_stream);
        if (isNew)
        {
            _writer.Write(Magic);
            _stream.Flush();
        }
    }

    /// <summary>Current on-disk size — drives the size-based checkpoint trigger.</summary>
    public long SizeBytes
    {
        get { using (_sync.Lock()) return _stream.Length; }
    }

    // Reused per-thread payload scratch: appends run at op-batch rate (thousands/s) and big batches made
    // the per-call MemoryStream a Large-Object-Heap allocation — LOH churn → fragmentation → the forced
    // blocking LOH-compact GC (a periodic global freeze). The scratch grows to the largest batch and stays.
    [ThreadStatic] private static MemoryStream? _tlPayloadMs;
    [ThreadStatic] private static BinaryWriter? _tlPayloadBw;

    /// <summary>Append one operation-batch record (buffered, not fsynced until <see cref="Commit"/>).</summary>
    public void Append(long lsn, Action<BinaryWriter> writePayload)
    {
        var ms = _tlPayloadMs ??= new MemoryStream(4096);
        var bw = _tlPayloadBw ??= new BinaryWriter(ms);
        ms.Position = 0;
        ms.SetLength(0);
        writePayload(bw);
        bw.Flush();
        var payload = ms.GetBuffer();
        var len = (int)ms.Length;

        using (_sync.Lock())
        {
            _writer.Write(RecordBatch);
            _writer.Write(lsn);
            _writer.Write(len);
            _writer.Write(payload, 0, len);
            _writer.Write(BatchChecksum(lsn, payload, len));
        }
    }

    /// <summary>Write a COMMIT marker covering everything appended so far and fsync — the durable commit.</summary>
    public void Commit(long uptoLsn)
    {
        using (_sync.Lock())
        {
            _writer.Write(RecordCommit);
            _writer.Write(uptoLsn);
            _writer.Write(CommitChecksum(uptoLsn));
            _stream.Flush(flushToDisk: true);
        }
    }

    /// <summary>
    /// Replays every committed batch with <c>lsn &gt; fromLsnExclusive</c> (up to the last valid COMMIT
    /// marker), in LSN order. Records after the last COMMIT, or a torn/corrupt tail, are uncommitted and
    /// skipped. Read-only — the log is kept until the next checkpoint truncates it.
    /// </summary>
    public void Recover(long fromLsnExclusive, Action<long, BinaryReader> replay)
    {
        using (_sync.Lock())
        {
            var batches = new List<(long Lsn, byte[] Payload)>();
            long lastCommitLsn = long.MinValue;

            foreach (var rec in ReadRecords())
            {
                if (rec.IsCommit)
                    lastCommitLsn = rec.Lsn; // uptoLsn
                else
                    batches.Add((rec.Lsn, rec.Payload!));
            }

            foreach (var (lsn, payload) in batches)
            {
                if (lsn <= fromLsnExclusive || lsn > lastCommitLsn) continue;
                using var ms = new MemoryStream(payload, writable: false);
                replay(lsn, new BinaryReader(ms));
            }

            _stream.Seek(0, SeekOrigin.End); // resume appending after the existing records
        }
    }

    /// <summary>
    /// Drops records whose LSN ≤ <paramref name="checkpointLsn"/> (now durable in the heap), keeping the
    /// raw tail (records with LSN &gt; checkpointLsn, including their COMMIT markers). Called by the
    /// checkpoint AFTER the heap header is durable.
    /// </summary>
    public void Truncate(long checkpointLsn)
    {
        using (_sync.Lock())
        {
            _stream.Flush();

            // Find the byte offset of the first batch with lsn > checkpointLsn (LSN is monotonic, so this
            // splits the file cleanly; everything before is checkpointed, everything after is kept).
            long cutoff = -1;
            foreach (var rec in ReadRecords())
            {
                if (!rec.IsCommit && rec.Lsn > checkpointLsn) { cutoff = rec.StartOffset; break; }
            }

            var tmp = _path + ".tmp";
            using (var outFs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                outFs.Write(Magic, 0, Magic.Length);
                if (cutoff >= 0)
                {
                    _stream.Seek(cutoff, SeekOrigin.Begin);
                    _stream.CopyTo(outFs);
                }
                outFs.Flush(flushToDisk: true);
            }

            _writer.Dispose(); // closes _stream
            File.Move(tmp, _path, overwrite: true);
            _stream = OpenAppend(_path, out _);
            _writer = new BinaryWriter(_stream);
            _stream.Seek(0, SeekOrigin.End);
        }
    }

    public void Dispose()
    {
        using (_sync.Lock())
        {
            _writer.Dispose();
        }
    }

    // ── Record parsing (shared by Recover + Truncate) ─────────────────────────

    private readonly record struct Record(bool IsCommit, long Lsn, byte[]? Payload, long StartOffset);

    private IEnumerable<Record> ReadRecords()
    {
        _stream.Flush();
        var savedPos = _stream.Position;
        _stream.Seek(0, SeekOrigin.Begin);
        var reader = new BinaryReader(_stream);
        try
        {
            var magic = reader.ReadBytes(Magic.Length);
            if (magic.Length != Magic.Length || !magic.AsSpan().SequenceEqual(Magic))
                yield break;

            while (_stream.Position < _stream.Length)
            {
                var start = _stream.Position;
                byte type;
                try { type = reader.ReadByte(); } catch { break; }

                if (type == RecordBatch)
                {
                    long lsn; int len; byte[] payload; int checksum;
                    try
                    {
                        lsn = reader.ReadInt64();
                        len = reader.ReadInt32();
                        if (len < 0 || len > 100_000_000) break;
                        payload = reader.ReadBytes(len);
                        if (payload.Length != len) break;
                        checksum = reader.ReadInt32();
                    }
                    catch { break; }
                    if (checksum != BatchChecksum(lsn, payload, len)) break; // torn tail
                    yield return new Record(false, lsn, payload, start);
                }
                else if (type == RecordCommit)
                {
                    long uptoLsn; int checksum;
                    try { uptoLsn = reader.ReadInt64(); checksum = reader.ReadInt32(); }
                    catch { break; }
                    if (checksum != CommitChecksum(uptoLsn)) break;
                    yield return new Record(true, uptoLsn, null, start);
                }
                else break;
            }
        }
        finally
        {
            _stream.Seek(savedPos, SeekOrigin.Begin);
        }
    }

    private static FileStream OpenAppend(string path, out bool isNew)
    {
        isNew = !File.Exists(path) || new FileInfo(path).Length == 0;
        return new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 65536);
    }

    // ── Checksums (FNV-1a, same shape as WalHeap) ─────────────────────────────

    private static int BatchChecksum(long lsn, byte[] buffer, int count)
    {
        unchecked
        {
            var hash = Mix(ChecksumSeed, lsn);
            hash ^= count; hash *= 16777619;
            for (var i = 0; i < count; i++) { hash ^= buffer[i]; hash *= 16777619; }
            return hash;
        }
    }

    private static int CommitChecksum(long uptoLsn) => Mix(ChecksumSeed, uptoLsn);

    private static int Mix(int seed, long value)
    {
        unchecked
        {
            var hash = seed;
            hash ^= (int)(value & 0xFFFFFFFF); hash *= 16777619;
            hash ^= (int)(value >> 32); hash *= 16777619;
            return hash;
        }
    }
}
