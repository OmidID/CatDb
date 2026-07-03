// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;
using CatDb.Database;
using CatDb.Storage;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// <see cref="DatabaseOptions.UseNativeLeafStorage"/>: leaf row data lives in unmanaged memory
/// (<see cref="CatDb.General.Collections.NativeOrderedSet"/>) instead of the managed ordered set —
/// keys + records off the GC heap. Covers what the 2026-06 stress hardening actually exercised:
/// primitive inline keys (long/int/DateTime) vs non-inline (string/composite), split/merge under
/// small leaves, overwrite/delete arena compaction, small-cache eviction (the deferred native-reclaim
/// path), and the raw-byte checkpoint passthrough (store → reopen round-trip).
/// </summary>
public class NativeLeafStorageTests : IDisposable
{
    private readonly string _file = Path.Combine(Path.GetTempPath(), $"catdb_native_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var f in new[] { _file, _file + ".oplog", _file + ".wal" })
            if (File.Exists(f)) File.Delete(f);
    }

    private static DatabaseOptions Native(
        long cacheSizeBytes = 2L * 1024 * 1024 * 1024,
        int maxRecordsPerLeaf = 8192,
        int minRecordsPerLeaf = 4096) => new()
    {
        CommitMode = CommitMode.TransactionLog,
        UseNativeLeafStorage = true,
        CacheSizeBytes = cacheSizeBytes,
        MaxRecordsPerLeaf = maxRecordsPerLeaf,
        MinRecordsPerLeaf = minRecordsPerLeaf,
    };

    [Fact]
    public void InlineLongKeys_RoundTrip_InMemory()
    {
        using var engine = CatDb.Database.CatDb.FromMemory(Native());
        var t = engine.OpenXTable<long, string>("t");
        for (var i = 0L; i < 2_000; i++) t[i] = $"v{i}";

        t.Count().Should().Be(2_000);
        for (var i = 0L; i < 2_000; i += 97)
            t[i].Should().Be($"v{i}");
    }

    [Fact]
    public void InlineIntKeys_And_DateTimeKeys_RoundTrip()
    {
        using var engine = CatDb.Database.CatDb.FromMemory(Native());

        var ints = engine.OpenXTable<int, int>("ints");
        for (var i = 0; i < 1_000; i++) ints[i] = i * i;
        for (var i = 0; i < 1_000; i += 31) ints[i].Should().Be(i * i);

        var dates = engine.OpenXTable<DateTime, string>("dates");
        var baseDate = new DateTime(2025, 1, 1);
        for (var i = 0; i < 500; i++) dates[baseDate.AddDays(i)] = $"day{i}";
        dates[baseDate.AddDays(250)].Should().Be("day250");
        dates.Count().Should().Be(500);
    }

    [Fact]
    public void NonInlineStringKeys_RoundTrip()
    {
        using var engine = CatDb.Database.CatDb.FromMemory(Native());
        var t = engine.OpenXTable<string, int>("t");
        for (var i = 0; i < 1_000; i++) t[$"key-{i:D5}"] = i;

        t.Count().Should().Be(1_000);
        t["key-00042"].Should().Be(42);
        t["key-00999"].Should().Be(999);
    }

    [Fact]
    public void CompositeKey_NonInline_RoundTrip()
    {
        using var engine = CatDb.Database.CatDb.FromMemory(Native());
        var t = engine.OpenXTable<Slots<string, int>, string>("t");
        for (var i = 0; i < 300; i++)
            t[new Slots<string, int>($"city{i % 5}", i)] = $"rec{i}";

        t.Count().Should().Be(300);
        t[new Slots<string, int>("city2", 2)].Should().Be("rec2");
    }

    [Fact]
    public void Splits_And_Merges_Under_SmallLeaves_PreserveAllData()
    {
        // Tiny leaves force frequent Split (native arena copy path) — then deletes force Merge
        // (native arena Merge/adopt path).
        using var engine = CatDb.Database.CatDb.FromMemory(Native(maxRecordsPerLeaf: 64, minRecordsPerLeaf: 16));
        var t = engine.OpenXTable<long, string>("t");

        const int n = 5_000;
        for (var i = 0L; i < n; i++) t[i] = $"v{i}";
        t.Count().Should().Be(n);

        // Delete every third key — forces underflow merges across many leaves.
        for (var i = 0L; i < n; i += 3) t.Delete(i);
        t.Count().Should().Be(n - (n / 3 + 1));

        // Full forward scan must be contiguous, ordered, and match the surviving set exactly.
        var expectedKeys = Enumerable.Range(0, n).Select(i => (long)i).Where(i => i % 3 != 0).ToList();
        var gotKeys = t.Forward().Select(kv => kv.Key).ToList();
        gotKeys.Should().Equal(expectedKeys);

        foreach (var k in gotKeys.Where((_, idx) => idx % 200 == 0))
            t[k].Should().Be($"v{k}");
    }

    [Fact]
    public void RepeatedOverwrite_SameKeys_ArenaCompactionKeepsCorrectValue()
    {
        // Repeatedly overwriting the same keys orphans old record bytes in the native arena
        // (OverwriteRecord/MaybeCompact path) — the LATEST value must always win.
        using var engine = CatDb.Database.CatDb.FromMemory(Native(maxRecordsPerLeaf: 128, minRecordsPerLeaf: 32));
        var t = engine.OpenXTable<long, string>("t");

        const int keys = 200;
        for (var pass = 0; pass < 50; pass++)
            for (var i = 0L; i < keys; i++)
                t[i] = $"pass{pass}-key{i}";

        t.Count().Should().Be(keys);
        for (var i = 0L; i < keys; i++)
            t[i].Should().Be($"pass49-key{i}");
    }

    [Fact]
    public void SmallCache_ForcesEviction_DataSurvivesReadBack()
    {
        // A tiny CacheSizeBytes forces the byte-budget evictor to unload native leaves mid-run —
        // exercises Node.NativeAllocatedBytes (eviction visibility) + NativeReclaim deferred
        // disposal + reload-from-heap. This is the exact bug class that caused the 2026-06 leak
        // (eviction was blind to native bytes, so it never fired and memory grew unbounded).
        using var engine = CatDb.Database.CatDb.FromFile(
            _file, Native(cacheSizeBytes: 256 * 1024, maxRecordsPerLeaf: 128, minRecordsPerLeaf: 32));
        var t = engine.OpenXTable<long, string>("t");

        const int n = 20_000;
        for (var i = 0L; i < n; i++)
        {
            t[i] = $"value-{i}-{new string('x', 32)}";
            if (i % 1000 == 0) engine.Commit();  // commit periodically → checkpoint → eviction can fire
        }
        engine.Commit();

        t.Count().Should().Be(n);
        for (var i = 0L; i < n; i += 173)
            t[i].Should().Be($"value-{i}-{new string('x', 32)}");
    }

    [Fact]
    public void Checkpoint_Reopen_RawBytePassthrough_RoundTrips()
    {
        // Exercises OrderedSetPersist's VERSION_NATIVE path (WriteRawTo/ReadRawFrom): the arena's
        // already-serialized bytes are blitted straight to/from disk with no per-row materialize.
        const int n = 10_000;
        using (var engine = CatDb.Database.CatDb.FromFile(_file, Native()))
        {
            var t = engine.OpenXTable<long, string>("t");
            for (var i = 0L; i < n; i++) t[i] = $"v{i}";
            engine.Commit();
        }

        using var reopened = CatDb.Database.CatDb.FromFile(_file, Native());
        var rt = reopened.OpenXTable<long, string>("t");
        rt.Count().Should().Be(n);
        rt[0L].Should().Be("v0");
        rt[n - 1].Should().Be($"v{n - 1}");
        for (var i = 0L; i < n; i += 271)
            rt[i].Should().Be($"v{i}");
    }

    [Fact]
    public void Backward_Scan_Ordered_MatchesForwardReversed()
    {
        using var engine = CatDb.Database.CatDb.FromMemory(Native(maxRecordsPerLeaf: 100, minRecordsPerLeaf: 25));
        var t = engine.OpenXTable<long, int>("t");
        for (var i = 0L; i < 3_000; i++) t[i] = (int)i;

        var forward = t.Forward().Select(kv => kv.Key).ToList();
        var backward = t.Backward().Select(kv => kv.Key).ToList();
        backward.Should().Equal(forward.AsEnumerable().Reverse());
    }

    [Fact]
    public void RangeDelete_RemovesExactWindow()
    {
        using var engine = CatDb.Database.CatDb.FromMemory(Native(maxRecordsPerLeaf: 100, minRecordsPerLeaf: 25));
        var t = engine.OpenXTable<long, int>("t");
        for (var i = 0L; i < 2_000; i++) t[i] = (int)i;

        t.Delete(500, 1500); // inclusive range delete

        t.Count().Should().Be(2_000 - 1001);
        t.Exists(499).Should().BeTrue();
        t.Exists(500).Should().BeFalse();
        t.Exists(1500).Should().BeFalse();
        t.Exists(1501).Should().BeTrue();
    }
}
