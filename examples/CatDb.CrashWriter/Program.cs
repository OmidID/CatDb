// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

// Out-of-process crash victim for the incremental-checkpoint data-safety battery. Opens a TransactionLog +
// IncrementalCheckpoint database, writes ascending keys, and commits every <commitEvery>. After each commit
// returns (the op-log is fsynced → that batch is durable) it prints "COMMITTED <maxKey>" and flushes stdout,
// so the parent test knows the exact durability watermark before it Kill()s this process. The process NEVER
// disposes — the parent kills it mid-write, simulating a real crash (kill -9 / power loss). Every key the
// parent saw committed MUST survive the parent's reopen+replay.
//
// Args: <dbFile> <total> <commitEvery> <maxRecordsPerLeaf> <checkpointMaxNodes> <checkpointBytes>

using CatDb.Database;
using CatDb.Storage;

if (args.Length < 6)
{
    Console.Error.WriteLine("usage: <dbFile> <total> <commitEvery> <maxRecordsPerLeaf> <checkpointMaxNodes> <checkpointBytes>");
    return 2;
}

var file = args[0];
var total = long.Parse(args[1]);
var commitEvery = long.Parse(args[2]);
var maxRecordsPerLeaf = int.Parse(args[3]);
var checkpointMaxNodes = int.Parse(args[4]);
var checkpointBytes = long.Parse(args[5]);

var options = new DatabaseOptions
{
    CommitMode = CommitMode.TransactionLog,
    IncrementalCheckpoint = true,
    CheckpointMaxNodes = checkpointMaxNodes,
    CheckpointIntervalMs = 1_000_000,
    CheckpointLogSizeBytes = checkpointBytes,
    MaxRecordsPerLeaf = maxRecordsPerLeaf,
    MinRecordsPerLeaf = Math.Max(2, maxRecordsPerLeaf / 4),
};

var engine = CatDb.Database.CatDb.FromFile(file, options);
var t = engine.OpenXTable<long, string>("t");

var output = Console.OpenStandardOutput();
var writer = new StreamWriter(output) { AutoFlush = false };

for (var i = 0L; i < total; i++)
{
    t[i] = $"v{i}";
    if (i % commitEvery == commitEvery - 1)
    {
        engine.Commit();                 // returns only once the op-log is fsynced → keys [0..i] durable
        writer.WriteLine($"COMMITTED {i}");
        writer.Flush();                  // tell the parent the new watermark, then keep writing into the kill
    }
}

engine.Commit();
writer.WriteLine($"COMMITTED {total - 1}");
writer.Flush();
// Intentionally NO Dispose — the parent kills us; if we reach here the parent just lets us exit dirty.
return 0;
