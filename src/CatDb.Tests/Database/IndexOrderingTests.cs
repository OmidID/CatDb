// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// Covers the engine-native streaming index scans: descending (backward) order,
/// exclusive bounds, and multi-page BatchedScan correctness (&gt; one page of 4096).
/// </summary>
public class IndexOrderingTests : IDisposable
{
    private readonly IStorageEngine _engine;

    public class Customer
    {
        public string Email { get; set; } = "";
        public string City { get; set; } = "";
        public int Age { get; set; }
        public string Name { get; set; } = "";
    }

    public IndexOrderingTests() => _engine = CatDb.Database.CatDb.FromMemory();
    public void Dispose() => _engine.Dispose();

    // ── Descending (backward) index scans ────────────────────────────────────

    [Fact]
    public void UniqueIndex_OrderByDescending_StreamsDescending()
    {
        var table = _engine.OpenXTable<int, Customer>("c");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);
        for (int i = 1; i <= 20; i++)
            table.Replace(i, new Customer { Email = $"user{i:D2}@x.com", Age = i });
        _engine.Commit();

        var emails = table.Query(c => c.Email)
            .AtLeast("user05@x.com").AtMost("user15@x.com")
            .OrderByDescending(c => c.Email)
            .Select(r => r.Value.Email)
            .ToList();

        emails.Should().BeInDescendingOrder();
        emails.First().Should().Be("user15@x.com");
        emails.Last().Should().Be("user05@x.com");
        emails.Should().HaveCount(11);
    }

    [Fact]
    public void NonUniqueIndex_OrderByDescending_StreamsDescending()
    {
        var table = _engine.OpenXTable<int, Customer>("c");
        table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);
        for (int i = 1; i <= 60; i++)
            table.Replace(i, new Customer { Age = i % 10, Name = $"n{i}" }); // ages 0..9, ~6 each
        _engine.Commit();

        var ages = table.Query(c => c.Age)
            .Between(2, 7)
            .OrderByDescending(c => c.Age)
            .Select(r => r.Value.Age)
            .ToList();

        ages.Should().OnlyContain(a => a >= 2 && a <= 7);
        ages.Should().BeInDescendingOrder();
        ages.First().Should().Be(7);
        ages.Last().Should().Be(2);
    }

    [Fact]
    public void IndexQuery_Backward_EnumeratesDescending()
    {
        var table = _engine.OpenXTable<int, Customer>("c");
        table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);
        for (int i = 1; i <= 30; i++)
            table.Replace(i, new Customer { Age = i });
        _engine.Commit();

        var ages = table.Query(c => c.Age).Backward().Take(5)
            .Select(r => r.Value.Age).ToList();

        ages.Should().ContainInOrder(30, 29, 28, 27, 26);
    }

    // ── Exclusive bounds (previously dropped on the index path) ───────────────

    [Fact]
    public void UniqueIndex_ExclusiveBounds_AreRespected()
    {
        var table = _engine.OpenXTable<int, Customer>("c");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);
        foreach (var (k, e) in new[] { (1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e") })
            table.Replace(k, new Customer { Email = e });
        _engine.Commit();

        // (b, d) exclusive → only c
        var got = table.Query(c => c.Email).GreaterThan("b").LessThan("d")
            .Select(r => r.Value.Email).ToList();

        got.Should().ContainSingle().Which.Should().Be("c");
    }

    [Fact]
    public void NonUniqueIndex_ExclusiveBounds_AreRespected()
    {
        var table = _engine.OpenXTable<int, Customer>("c");
        table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);
        for (int i = 1; i <= 50; i++)
            table.Replace(i, new Customer { Age = i % 10 }); // ages 0..9
        _engine.Commit();

        // Between 3 and 6, both exclusive → ages {4, 5}
        var ages = table.Query(c => c.Age).Between(3, 6, fromInclusive: false, toInclusive: false)
            .Select(r => r.Value.Age).Distinct().OrderBy(a => a).ToList();

        ages.Should().Equal(4, 5);
    }

    // ── Multi-page BatchedScan correctness (> one 4096 page) ──────────────────

    [Fact]
    public void UniqueIndex_FullScan_AcrossManyPages_IsOrderedAndComplete()
    {
        var table = _engine.OpenXTable<int, Customer>("big");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);
        const int n = 10_000; // > 2 * 4096 → exercises re-seek + duplicate-skip
        for (int i = 0; i < n; i++)
            table.Replace(i, new Customer { Email = $"user{i:D6}@x.com" });
        _engine.Commit();

        var asc = table.Query(c => c.Email).Select(r => r.Value.Email).ToList();
        asc.Should().HaveCount(n);
        asc.Should().BeInAscendingOrder();
        asc.Should().OnlyHaveUniqueItems();

        var desc = table.Query(c => c.Email).OrderByDescending(c => c.Email)
            .Select(r => r.Value.Email).ToList();
        desc.Should().HaveCount(n);
        desc.Should().BeInDescendingOrder();
    }

    [Fact]
    public void NonUniqueIndex_EqualityBlock_LargerThanPage_ReturnsAll()
    {
        var table = _engine.OpenXTable<int, Customer>("big");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);
        const int n = 5_000; // single equal-field block > 4096 page
        for (int i = 0; i < n; i++)
            table.Replace(i, new Customer { City = "NYC" });
        _engine.Commit();

        table.CountByIndex<int, Customer, string>("City", "NYC").Should().Be(n);
        table.Query(c => c.City).Equals("NYC").Count().Should().Be(n);
        table.Query(c => c.City).Equals("NYC").ToList().Should().HaveCount(n);
    }

    [Fact]
    public void NonUniqueIndex_DescendingRange_AcrossPages_IsOrdered()
    {
        var table = _engine.OpenXTable<int, Customer>("big");
        table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);
        const int n = 10_000;
        for (int i = 0; i < n; i++)
            table.Replace(i, new Customer { Age = i % 50 }); // 200 rows per age, ages 0..49
        _engine.Commit();

        // ages 20..40 inclusive → 21 * 200 = 4200 rows > one page
        var ages = table.Query(c => c.Age).Between(20, 40)
            .OrderByDescending(c => c.Age)
            .Select(r => r.Value.Age).ToList();

        ages.Should().HaveCount(4200);
        ages.Should().BeInDescendingOrder();
        ages.First().Should().Be(40);
        ages.Last().Should().Be(20);
    }

    // ── Concurrency smoke: reads while writes happen (deadlock guard) ─────────

    [Fact]
    public async Task StreamingIndexScan_DoesNotDeadlock_UnderConcurrentWrites()
    {
        var table = _engine.OpenXTable<int, Customer>("c");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);
        for (int i = 0; i < 2_000; i++)
            table.Replace(i, new Customer { City = i % 2 == 0 ? "NYC" : "LA", Age = i });
        _engine.Commit();

        using var cts = new CancellationTokenSource();
        var writer = Task.Run(() =>
        {
            var r = new Random(1);
            while (!cts.IsCancellationRequested)
            {
                var k = r.Next(0, 2_000);
                table.Replace(k, new Customer { City = r.Next(2) == 0 ? "NYC" : "LA", Age = k });
            }
        });

        try
        {
            for (int iter = 0; iter < 50; iter++)
            {
                // Interleaves index-table scan with main-table point lookups.
                var count = table.Query(c => c.City).Equals("NYC").Count();
                count.Should().BeGreaterThanOrEqualTo(0);
            }
        }
        finally
        {
            cts.Cancel();
            var finished = await Task.WhenAny(writer, Task.Delay(TimeSpan.FromSeconds(5))) == writer;
            finished.Should().BeTrue("writer should stop without deadlock");
        }
    }
}
