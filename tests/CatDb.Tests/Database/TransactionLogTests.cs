// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Storage;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// <see cref="CommitMode.TransactionLog"/>: a commit fsyncs the logical op-log (no node store under the
/// root lock); dirty nodes flush at an occasional checkpoint that truncates the log. On reopen the heap
/// (state ≤ last checkpoint) is loaded and the log tail is replayed. These tests prove committed data
/// survives a reopen both BEFORE a checkpoint (pure log replay) and AFTER one (heap + truncated log).
/// </summary>
public class TransactionLogTests : IDisposable
{
    private readonly string _file = Path.Combine(Path.GetTempPath(), $"catdb_txlog_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var f in new[] { _file, _file + ".oplog", _file + ".wal" })
            if (File.Exists(f)) File.Delete(f);
    }

    private DatabaseOptions Options(int checkpointMs = 1_000_000, long checkpointBytes = long.MaxValue) => new()
    {
        CommitMode = CommitMode.TransactionLog,
        CheckpointIntervalMs = checkpointMs,       // huge → no time checkpoint
        CheckpointLogSizeBytes = checkpointBytes,  // huge → no size checkpoint
    };

    [Fact]
    public void Commit_NoCheckpoint_ReopenReplaysLog()
    {
        // Checkpoint thresholds set absurdly high → no checkpoint fires → recovery is PURE log replay.
        using (var engine = CatDb.Database.CatDb.FromFile(_file, Options()))
        {
            var t = engine.OpenXTable<long, string>("t");
            for (var i = 0L; i < 5_000; i++)
                t[i] = $"v{i}";
            engine.Commit();
        }

        using var reopened = CatDb.Database.CatDb.FromFile(_file, Options());
        var rt = reopened.OpenXTable<long, string>("t");
        rt.Count().Should().Be(5_000);
        rt[0L].Should().Be("v0");
        rt[4_999L].Should().Be("v4999");
    }

    [Fact]
    public void Checkpoint_ThenMoreCommits_ReopenRecoversAll()
    {
        // Tiny size budget → checkpoints fire often → recovery = heap (≤ last checkpoint) + replayed tail.
        var opts = Options(checkpointBytes: 64 * 1024);
        using (var engine = CatDb.Database.CatDb.FromFile(_file, opts))
        {
            var t = engine.OpenXTable<long, string>("t");
            for (var c = 0; c < 40; c++)
            {
                for (var i = 0L; i < 500; i++)
                    t[c * 500 + i] = $"v{c * 500 + i}";
                engine.Commit(); // many commits → several checkpoints + log truncations
            }
        }

        using var reopened = CatDb.Database.CatDb.FromFile(_file, opts);
        var rt = reopened.OpenXTable<long, string>("t");
        rt.Count().Should().Be(20_000);
        rt[0L].Should().Be("v0");
        rt[19_999L].Should().Be("v19999");
    }

    [Fact]
    public void MultiTable_Reopen_AllTablesRecovered()
    {
        using (var engine = CatDb.Database.CatDb.FromFile(_file, Options()))
        {
            var a = engine.OpenXTable<long, string>("a");
            var b = engine.OpenXTable<int, string>("b");
            for (var i = 0L; i < 1_000; i++) a[i] = $"a{i}";
            for (var i = 0; i < 1_000; i++) b[i] = $"b{i}";
            engine.Commit();
        }

        using var reopened = CatDb.Database.CatDb.FromFile(_file, Options());
        reopened.OpenXTable<long, string>("a").Count().Should().Be(1_000);
        reopened.OpenXTable<int, string>("b").Count().Should().Be(1_000);
        reopened.OpenXTable<long, string>("a")[999L].Should().Be("a999");
    }
}
