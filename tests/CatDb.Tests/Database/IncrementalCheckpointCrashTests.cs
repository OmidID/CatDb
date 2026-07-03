// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Diagnostics;
using CatDb.Database;
using CatDb.Storage;
using FluentAssertions;
using Xunit;

namespace CatDb.Tests.Database;

/// <summary>
/// The strongest data-safety test for <see cref="DatabaseOptions.IncrementalCheckpoint"/>: a real
/// out-of-process crash. A child process (CatDb.CrashWriter) opens an incremental TransactionLog database,
/// writes ascending keys, commits in batches, and prints the durable watermark after each commit — then this
/// test <see cref="Process.Kill(bool)"/>s it mid-write (no Dispose, no clean shutdown = kill -9 / power loss).
/// On reopen, every key the child reported committed MUST be present and correct, and a full scan must be
/// gap-free. Repeated at several kill points to shake out the "lose data months later" class of bug.
/// </summary>
public class IncrementalCheckpointCrashTests
{
    private static string CrashWriterDll()
    {
        // Test bin: .../tests/CatDb.Tests/bin/<config>/<tfm>/
        //   → CrashWriter bin: .../examples/CatDb.CrashWriter/bin/<config>/<tfm>/
        var baseDir = AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var tfm = Path.GetFileName(baseDir);                                   // net10.0
        var config = Path.GetFileName(Path.GetDirectoryName(baseDir)!);        // Debug / Release
        var repoRoot = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "..", "..")); // repo root
        var dll = Path.Combine(repoRoot, "examples", "CatDb.CrashWriter", "bin", config, tfm, "CatDb.CrashWriter.dll");
        File.Exists(dll).Should().BeTrue($"crash-writer must be built at {dll}");
        return dll;
    }

    [Theory]
    [InlineData(2)]    // kill very early — heap barely seeded, recovery is almost all replay
    [InlineData(9)]
    [InlineData(30)]
    [InlineData(75)]   // kill deep — many partial checkpoints + leaf/internal splits behind the crash
    public void Kill_MidWrite_ReopenRecoversEveryCommittedKey(int killAfterCommits)
    {
        var file = Path.Combine(Path.GetTempPath(), $"catdb_crash_{Guid.NewGuid():N}.db");
        try
        {
            // Tiny leaves + tiny flush bound + small log budget → constant splits, deeply partial checkpoints.
            var psi = new ProcessStartInfo("dotnet")
            {
                RedirectStandardOutput = true,
                UseShellExecute = false,
            };
            foreach (var a in new[] { CrashWriterDll(), file, "1000000", "200", "128", "2", "16384" })
                psi.ArgumentList.Add(a);

            using var child = Process.Start(psi)!;

            long watermark = -1;
            var seen = 0;
            string? line;
            while (seen < killAfterCommits && (line = child.StandardOutput.ReadLine()) != null)
            {
                if (line.StartsWith("COMMITTED ", StringComparison.Ordinal))
                {
                    watermark = long.Parse(line.AsSpan("COMMITTED ".Length));
                    seen++;
                }
            }

            child.Kill(true);          // crash: no Dispose, possibly mid-commit on the next (uncommitted) batch
            child.WaitForExit(15_000).Should().BeTrue("child should terminate after Kill");
            watermark.Should().BeGreaterThan(-1, "child must have committed at least one batch before the kill");

            // Reopen and verify: every committed key survives, values exact, scan gap-free up to the watermark.
            var opts = new DatabaseOptions
            {
                CommitMode = CommitMode.TransactionLog,
                IncrementalCheckpoint = true,
                CheckpointMaxNodes = 2,
                MaxRecordsPerLeaf = 128,
                MinRecordsPerLeaf = 32,
            };
            using var engine = CatDb.Database.CatDb.FromFile(file, opts);
            var t = engine.OpenXTable<long, string>("t");

            // Spot-check + full contiguous scan of the durable prefix [0..watermark].
            t.Count().Should().BeGreaterThanOrEqualTo(watermark + 1);
            var next = 0L;
            foreach (var row in t.Forward())
            {
                if (row.Key > watermark) break;   // keys beyond the watermark may or may not be present — don't assert
                row.Key.Should().Be(next, "scan of the committed prefix must be gap-free");
                row.Value.Should().Be($"v{row.Key}");
                next++;
            }
            next.Should().Be(watermark + 1, "every committed key in [0..watermark] must be present and contiguous");
        }
        finally
        {
            foreach (var f in new[] { file, file + ".oplog", file + ".wal" })
                if (File.Exists(f)) File.Delete(f);
        }
    }
}
