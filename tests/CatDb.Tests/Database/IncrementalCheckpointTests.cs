// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Storage;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// <see cref="DatabaseOptions.IncrementalCheckpoint"/>: each checkpoint flushes only the coldest
/// <see cref="DatabaseOptions.CheckpointMaxNodes"/> dirty leaves (plus the root, every dirty internal, and
/// every never-stored split product) and advances the recovery boundary to just below the oldest op still
/// unflushed in a leaf; the rest stay in the op-log and are replayed (idempotently, in LSN order) on reopen.
///
/// These tests are the DATA-SAFETY battery: with a deliberately tiny bound, most leaves are NEVER flushed by
/// a checkpoint, so a reopen is forced down the partial-checkpoint → log-replay path. <see cref="WTree"/>
/// Dispose does NOT perform a final full flush (it closes the heap at the last checkpoint + the op-log), so a
/// clean close here behaves like a crash right after the last commit: the heap is incomplete on purpose and
/// correctness depends entirely on replay. Every committed key MUST survive.
/// </summary>
public class IncrementalCheckpointTests : IDisposable
{
    private readonly string _file = Path.Combine(Path.GetTempPath(), $"catdb_incr_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var f in new[] { _file, _file + ".oplog", _file + ".wal" })
            if (File.Exists(f)) File.Delete(f);
    }

    // Incremental ON, a tiny flush bound and tiny leaves so checkpoints fire constantly yet flush almost
    // nothing — forcing the replay path. checkpointBytes small → a checkpoint per few commits.
    private static DatabaseOptions Incremental(long checkpointBytes = 16 * 1024, int maxNodes = 2) => new()
    {
        CommitMode = CommitMode.TransactionLog,
        IncrementalCheckpoint = true,
        CheckpointMaxNodes = maxNodes,
        CheckpointIntervalMs = 1_000_000,        // no time-based checkpoint — drive purely by size
        CheckpointLogSizeBytes = checkpointBytes, // small → frequent, deliberately-partial checkpoints
        MaxRecordsPerLeaf = 256,                  // small leaves → many leaves + frequent splits
        MinRecordsPerLeaf = 64,
    };

    [Fact]
    public void Incremental_PartialCheckpoints_ReopenRecoversAll()
    {
        const int n = 20_000;
        using (var engine = CatDb.Database.CatDb.FromFile(_file, Incremental()))
        {
            var t = engine.OpenXTable<long, string>("t");
            for (var i = 0L; i < n; i++)
            {
                t[i] = $"v{i}";
                if (i % 500 == 0) engine.Commit(); // many commits → many partial checkpoints
            }
            engine.Commit();
        }

        using var reopened = CatDb.Database.CatDb.FromFile(_file, Incremental());
        var rt = reopened.OpenXTable<long, string>("t");
        rt.Count().Should().Be(n);
        for (var i = 0L; i < n; i += 137)
            rt[i].Should().Be($"v{i}");
        rt[n - 1].Should().Be($"v{n - 1}");
    }

    [Fact]
    public void Incremental_NoCheckpoint_PureReplay()
    {
        // Huge thresholds → no checkpoint fires at all → recovery is pure log replay (incremental flag inert
        // on the write path, exercised only at the would-be checkpoint that never comes).
        var opts = Incremental(checkpointBytes: long.MaxValue);
        using (var engine = CatDb.Database.CatDb.FromFile(_file, opts))
        {
            var t = engine.OpenXTable<long, string>("t");
            for (var i = 0L; i < 5_000; i++) t[i] = $"v{i}";
            engine.Commit();
        }

        using var reopened = CatDb.Database.CatDb.FromFile(_file, opts);
        var rt = reopened.OpenXTable<long, string>("t");
        rt.Count().Should().Be(5_000);
        rt[0L].Should().Be("v0");
        rt[4_999L].Should().Be("v4999");
    }

    [Fact]
    public void Incremental_UpdatesAndDeletes_ReopenReflectsLatest()
    {
        // Idempotent in-order replay must yield the LAST committed value for each key, and deletes must stay
        // deleted — even though flushed-ahead leaves may hold intermediate states on disk.
        using (var engine = CatDb.Database.CatDb.FromFile(_file, Incremental()))
        {
            var t = engine.OpenXTable<long, string>("t");
            for (var i = 0L; i < 10_000; i++) t[i] = $"first{i}";
            engine.Commit();
            for (var i = 0L; i < 10_000; i++) t[i] = $"second{i}"; // overwrite every key
            engine.Commit();
            for (var i = 0L; i < 10_000; i += 2) t.Delete(i);       // delete the evens
            engine.Commit();
        }

        using var reopened = CatDb.Database.CatDb.FromFile(_file, Incremental());
        var rt = reopened.OpenXTable<long, string>("t");
        rt.Count().Should().Be(5_000);
        rt[1L].Should().Be("second1");
        rt[9_999L].Should().Be("second9999");
        rt.TryGet(0L, out _).Should().BeFalse();
        rt.TryGet(4_000L, out _).Should().BeFalse();
    }

    [Fact]
    public void Incremental_ManySplits_StructuralRecovery()
    {
        // Sequential ascending keys with tiny leaves → continuous right-edge splits, so the tree gains depth
        // and many internal nodes while only a bounded subset of leaves is ever checkpoint-flushed. Reopen
        // must rebuild the full structure from the heap (partial) + replayed log with NO dangling child
        // handle and every key present.
        const int n = 50_000;
        using (var engine = CatDb.Database.CatDb.FromFile(_file, Incremental(checkpointBytes: 8 * 1024)))
        {
            var t = engine.OpenXTable<long, string>("t");
            for (var i = 0L; i < n; i++)
            {
                t[i] = $"v{i}";
                if (i % 1000 == 999) engine.Commit();
            }
            engine.Commit();
        }

        using var reopened = CatDb.Database.CatDb.FromFile(_file, Incremental(checkpointBytes: 8 * 1024));
        var rt = reopened.OpenXTable<long, string>("t");
        rt.Count().Should().Be(n);
        // Full forward scan must be contiguous and complete.
        var expected = 0L;
        foreach (var row in rt.Forward())
        {
            row.Key.Should().Be(expected);
            row.Value.Should().Be($"v{expected}");
            expected++;
        }
        expected.Should().Be(n);
    }

    [Fact]
    public void Incremental_MultiTable_ReopenRecoversAll()
    {
        using (var engine = CatDb.Database.CatDb.FromFile(_file, Incremental()))
        {
            var a = engine.OpenXTable<long, string>("a");
            var b = engine.OpenXTable<int, string>("b");
            for (var i = 0L; i < 8_000; i++)
            {
                a[i] = $"a{i}";
                if (i < 8_000) b[(int)i] = $"b{i}";
                if (i % 400 == 0) engine.Commit();
            }
            engine.Commit();
        }

        using var reopened = CatDb.Database.CatDb.FromFile(_file, Incremental());
        var ra = reopened.OpenXTable<long, string>("a");
        var rb = reopened.OpenXTable<int, string>("b");
        ra.Count().Should().Be(8_000);
        rb.Count().Should().Be(8_000);
        ra[7_999L].Should().Be("a7999");
        rb[7_999].Should().Be("b7999");
    }
}
