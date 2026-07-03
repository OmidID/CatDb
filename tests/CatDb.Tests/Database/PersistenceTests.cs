// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Tests.Data;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// Tests for file-based persistence: data written before Commit() must survive
/// reopening the engine from the same file. This was a primary crash area
/// after the net6 migration.
/// </summary>
public class PersistenceTests : IDisposable
{
    private readonly string _filePath;

    public PersistenceTests()
    {
        _filePath = Path.Combine(Path.GetTempPath(), $"catdb_test_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_filePath))
            File.Delete(_filePath);
    }

    [Fact]
    public void WriteAndReopen_Long_String_DataSurvives()
    {
        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var table = engine.OpenXTable<long, string>("persist");
            for (var i = 1L; i <= 100L; i++)
                table[i] = $"value-{i}";
            engine.Commit();
        }

        using var readEngine = CatDb.Database.CatDb.FromFile(_filePath);
        var readTable = readEngine.OpenXTable<long, string>("persist");
        readTable.Count().Should().Be(100);
        readTable[1L].Should().Be("value-1");
        readTable[100L].Should().Be("value-100");
    }

    [Fact]
    public void WriteAndReopen_ComplexClass_DataSurvives()
    {
        var tick = new Tick("MSFT", new DateTime(2025, 3, 10, 14, 0, 0), 410.5, 410.6, 50, 75, "Reuters");

        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var table = engine.OpenXTable<long, Tick>("ticks");
            table[1L] = tick;
            engine.Commit();
        }

        using var readEngine = CatDb.Database.CatDb.FromFile(_filePath);
        var readTable = readEngine.OpenXTable<long, Tick>("ticks");
        var result = readTable[1L];
        result.Should().BeEquivalentTo(tick);
    }

    [Fact]
    public void MultipleTablesInOneFile_AllSurviveReopen()
    {
        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t1 = engine.OpenXTable<int, string>("table1");
            var t2 = engine.OpenXTable<string, int>("table2");
            t1[1] = "hello";
            t2["world"] = 42;
            engine.Commit();
        }

        using var readEngine = CatDb.Database.CatDb.FromFile(_filePath);
        var rt1 = readEngine.OpenXTable<int, string>("table1");
        var rt2 = readEngine.OpenXTable<string, int>("table2");
        rt1[1].Should().Be("hello");
        rt2["world"].Should().Be(42);
    }

    [Fact]
    public void DeleteAndReopen_DeletedKeyGone_RemainingKeyPresent()
    {
        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var table = engine.OpenXTable<int, string>("t");
            table[1] = "keep";
            table[2] = "delete-me";
            engine.Commit();
            table.Delete(2);
            engine.Commit();
        }

        using var readEngine = CatDb.Database.CatDb.FromFile(_filePath);
        var readTable = readEngine.OpenXTable<int, string>("t");
        readTable.Exists(1).Should().BeTrue();
        readTable.Exists(2).Should().BeFalse();
    }

    [Fact]
    public void UncommittedData_IsLostOnReopen()
    {
        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var table = engine.OpenXTable<int, string>("t");
            table[1] = "committed";
            engine.Commit();
            table[2] = "NOT-committed";
            // no Commit() — engine closed without commit
        }

        using var readEngine = CatDb.Database.CatDb.FromFile(_filePath);
        var readTable = readEngine.OpenXTable<int, string>("t");
        readTable.Exists(1).Should().BeTrue();
        readTable.Exists(2).Should().BeFalse();
    }

    [Fact]
    public void LargeDataset_WritesAndReadsCorrectly()
    {
        const int count = 10_000;

        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var table = engine.OpenXTable<long, Tick>("ticks");
            var rng = new Random(42);
            for (var i = 0L; i < count; i++)
            {
                table[i] = new Tick(
                    $"SYM{i % 10}",
                    DateTime.UtcNow.AddSeconds(i),
                    100.0 + rng.NextDouble(),
                    100.1 + rng.NextDouble(),
                    rng.Next(1, 1000),
                    rng.Next(1, 1000),
                    "TestProvider"
                );
            }
            engine.Commit();
        }

        using var readEngine = CatDb.Database.CatDb.FromFile(_filePath);
        var readTable = readEngine.OpenXTable<long, Tick>("ticks");
        readTable.Count().Should().Be(count);
        readTable.Forward().Select(kv => kv.Key).Should().BeInAscendingOrder();
    }
}
