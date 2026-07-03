// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Storage;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// The <see cref="CommitDurability"/> strategies must all persist identical, correct data — the only
/// difference is HOW dirty nodes are written (inline vs parallel), never WHAT. ParallelCheckpoint
/// serialises many nodes across threads, so this exercises a multi-leaf, multi-table commit and reopens.
/// </summary>
public class CommitDurabilityTests : IDisposable
{
    private readonly string _filePath = Path.Combine(Path.GetTempPath(), $"catdb_durab_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var f in new[] { _filePath, _filePath + ".wal" })
            if (File.Exists(f)) File.Delete(f);
    }

    [Theory]
    [InlineData(CommitDurability.Synchronous)]
    [InlineData(CommitDurability.ParallelCheckpoint)]
    [InlineData(CommitDurability.AsyncDeferred)]
    public void WriteReopen_AllModes_PersistIdenticalData(CommitDurability durability)
    {
        const int n = 20_000; // enough to span many leaves → real multi-node commit
        var options = new DatabaseOptions { CommitDurability = durability };

        using (var engine = CatDb.Database.CatDb.FromFile(_filePath, options))
        {
            var ticks = engine.OpenXTable<long, string>("ticks");
            var meta = engine.OpenXTable<int, string>("meta");
            for (var i = 0L; i < n; i++)
                ticks[i] = $"v{i}";
            for (var i = 0; i < 500; i++)
                meta[i] = $"m{i}";
            engine.Commit();
        }

        using var reopened = CatDb.Database.CatDb.FromFile(_filePath, options);
        var t = reopened.OpenXTable<long, string>("ticks");
        var m = reopened.OpenXTable<int, string>("meta");
        t.Count().Should().Be(n);
        m.Count().Should().Be(500);
        t[0L].Should().Be("v0");
        t[n - 1].Should().Be($"v{n - 1}");
        m[499].Should().Be("m499");
    }

    [Fact]
    public void AsyncDeferred_ManyCommits_AllDurableAfterCleanClose()
    {
        var options = new DatabaseOptions { CommitDurability = CommitDurability.AsyncDeferred };

        using (var engine = CatDb.Database.CatDb.FromFile(_filePath, options))
        {
            var t = engine.OpenXTable<long, string>("t");
            // Many separate commits exercise the background checkpoint + BeginCommit hand-off repeatedly.
            for (var c = 0; c < 50; c++)
            {
                for (var i = 0L; i < 200; i++)
                    t[c * 200 + i] = $"v{c * 200 + i}";
                engine.Commit();
            }
        } // clean close must flush the last in-flight checkpoint

        using var reopened = CatDb.Database.CatDb.FromFile(_filePath, options);
        var rt = reopened.OpenXTable<long, string>("t");
        rt.Count().Should().Be(10_000);
        rt[0L].Should().Be("v0");
        rt[9_999L].Should().Be("v9999");
    }
}
