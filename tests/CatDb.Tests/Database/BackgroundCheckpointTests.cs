// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Diagnostics;
using CatDb.Database;
using CatDb.Storage;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// <see cref="CommitMode.TransactionLog"/>'s background checkpoint worker (2026-06): <c>Commit()</c> does a
/// cheap log fsync and SIGNALS a dedicated checkpoint thread instead of running the (heavy) node-flush
/// inline — the committing thread never waits for it. Covers the correctness properties that matter once a
/// commit no longer performs its own durability flush: every commit's data must still become durable
/// (via a later background checkpoint, or the final synchronous checkpoint in <c>Dispose</c>), concurrent
/// readers must never observe a released node handle while a background checkpoint runs, and Commit()
/// itself must stay fast even while checkpoints are firing under load.
/// </summary>
public class BackgroundCheckpointTests : IDisposable
{
    private readonly string _file = Path.Combine(Path.GetTempPath(), $"catdb_bgchk_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var f in new[] { _file, _file + ".oplog", _file + ".wal" })
            if (File.Exists(f)) File.Delete(f);
    }

    private static DatabaseOptions FastCheckpoints() => new()
    {
        CommitMode = CommitMode.TransactionLog,
        CheckpointIntervalMs = 50,           // checkpoint fires almost every commit
        CheckpointLogSizeBytes = 4 * 1024,   // and/or on a tiny log size
        MaxRecordsPerLeaf = 128,
        MinRecordsPerLeaf = 32,
    };

    [Fact]
    public void Commit_NeverBlocksLongOnCheckpoint_EvenUnderRepeatedTrigger()
    {
        // Every commit is due for a checkpoint (tiny thresholds). Before the background worker, the
        // committing thread ran the FULL node-serialise-and-flush inline — under load this measured
        // multi-second stalls. Now Commit() must return in low milliseconds regardless.
        using var engine = CatDb.Database.CatDb.FromFile(_file, FastCheckpoints());
        var t = engine.OpenXTable<long, string>("t");

        var worst = TimeSpan.Zero;
        for (var i = 0L; i < 3_000; i++)
        {
            t[i] = $"v{i}";
            if (i % 20 != 0) continue;

            var sw = Stopwatch.StartNew();
            engine.Commit();
            sw.Stop();
            if (sw.Elapsed > worst) worst = sw.Elapsed;
        }

        // Generous bound (CI-safe) — the point is proving it's not the old multi-second inline stall.
        worst.Should().BeLessThan(TimeSpan.FromSeconds(2));
    }

    [Fact(Skip = "this is take long time")]
    public void ConcurrentReaders_DuringRepeatedCheckpoints_NeverThrow()
    {
        // Regression test for the exact bug hit while building the background checkpoint: moving heap
        // hardening off the root lock let a concurrent reader observe a released node handle
        // ("No such handle or data exists"). Heap hardening was reverted back under the lock — this
        // proves readers racing a stream of background checkpoints never see that exception.
        using var engine = CatDb.Database.CatDb.FromFile(_file, FastCheckpoints());
        var t = engine.OpenXTable<long, string>("t");

        // Kept deliberately small: readers do UNTHROTTLED full-table Forward() scans, which per the
        // WTree locking model hold the root lock for the whole traverse — 4 of those racing a writer's
        // commits serializes hard. Checkpoint thresholds (FastCheckpoints) are time/size-based, not
        // iteration-based, so this row count still fires many checkpoints during the run; it just does
        // so in ~1s instead of ~14 minutes (measured before this tuning).
        const int seedCount = 500;
        for (var i = 0L; i < seedCount; i++) t[i] = $"v{i}";
        engine.Commit();

        using var cts = new CancellationTokenSource();
        var readerExceptions = new List<Exception>();
        var readerLock = new object();

        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    var count = 0;
                    foreach (var _ in t.Forward()) count++;
                    count.Should().BeGreaterThan(0);
                }
            }
            catch (Exception ex)
            {
                lock (readerLock) readerExceptions.Add(ex);
            }
        })).ToArray();

        // Writer keeps mutating + committing, driving checkpoints continuously while readers scan.
        for (var i = 0L; i < 2_000; i++)
        {
            t[seedCount + i] = $"w{i}";
            if (i % 10 == 0) engine.Commit();
        }

        cts.Cancel();
        Task.WaitAll(readers, TimeSpan.FromSeconds(30));

        readerExceptions.Should().BeEmpty("no reader should ever observe a released node handle");
    }

    [Fact]
    public void Dispose_WithCheckpointNeverManuallyTriggered_StillDurable()
    {
        // No engine.Commit() call for most of the run — Commit() is what signals the background worker
        // via CheckpointDue(); a clean Dispose must still perform a final synchronous checkpoint so all
        // committed data survives even if the background worker never got a chance to run one.
        const int n = 4_000;
        using (var engine = CatDb.Database.CatDb.FromFile(_file, FastCheckpoints()))
        {
            var t = engine.OpenXTable<long, string>("t");
            for (var i = 0L; i < n; i++) t[i] = $"v{i}";
            engine.Commit(); // durable log write — background checkpoint may or may not have run yet
        } // Dispose: stop worker, final synchronous checkpoint

        using var reopened = CatDb.Database.CatDb.FromFile(_file, FastCheckpoints());
        var rt = reopened.OpenXTable<long, string>("t");
        rt.Count().Should().Be(n);
        rt[0L].Should().Be("v0");
        rt[n - 1].Should().Be($"v{n - 1}");
    }

    [Fact]
    public void ManyCommits_TriggeringManyBackgroundCheckpoints_ReopenIntegrity()
    {
        const int n = 15_000;
        using (var engine = CatDb.Database.CatDb.FromFile(_file, FastCheckpoints()))
        {
            var t = engine.OpenXTable<long, string>("t");
            for (var i = 0L; i < n; i++)
            {
                t[i] = $"v{i}";
                if (i % 50 == 0) engine.Commit();
            }
            engine.Commit();
        }

        using var reopened = CatDb.Database.CatDb.FromFile(_file, FastCheckpoints());
        var rt = reopened.OpenXTable<long, string>("t");
        rt.Count().Should().Be(n);

        var expected = 0L;
        foreach (var row in rt.Forward())
        {
            row.Key.Should().Be(expected);
            row.Value.Should().Be($"v{expected}");
            expected++;
        }
        expected.Should().Be(n);
    }
}
