// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// `WHERE a=v ORDER BY b[,c…]` must be served by the (a,b[,c…]) composite index as an ordered prefix scan
/// (no Sort, no fetch-everything) — the fix for the O(matches) "seek pks then fetch every match to sort" plan
/// that degraded index/sort queries as tables grew. These tests are CORRECTNESS-first: the optimized result
/// must equal a brute-force recount/ordering for every shape (asc, desc, Take, Skip, no-match), and the plan
/// must actually be the prefix scan where expected.
/// </summary>
public class CompositePrefixScanTests : IDisposable
{
    private readonly IStorageEngine _engine = CatDb.Database.CatDb.FromMemory();
    public void Dispose() => _engine.Dispose();

    public class Product
    {
        public string Category { get; set; } = "";
        public int Stock { get; set; }
        public double Price { get; set; }
    }

    private const int N = 2000;
    private static readonly string[] Cats = { "alpha", "bravo", "charlie", "delta" };

    private ITable<long, Product> Seed()
    {
        var t = _engine.OpenXTable<long, Product>("items");
        t.CreateIndex("Category", p => p.Category, IndexType.NonUnique);
        t.CreateIndex("Stock", p => p.Stock, IndexType.NonUnique);
        t.CreateIndex("CategoryStock", new[] { "Category", "Stock" }, IndexType.NonUnique);
        var rng = new Random(42);
        for (var i = 0L; i < N; i++)
            t.Replace(i, new Product { Category = Cats[(int)(i % 4)], Stock = rng.Next(0, 100), Price = rng.NextDouble() * 1000 });
        _engine.Commit();
        return t;
    }

    // brute-force reference: every row of the table, filtered + ordered in-memory.
    private List<KeyValuePair<long, Product>> All(ITable<long, Product> t)
        => t.Forward().ToList();

    [Fact]
    public void PrefixScan_IsChosen_ForFilterEqualPlusOrderByTrailing()
    {
        var plan = Seed().Query(p => p.Category).Equal("bravo").OrderBy(p => p.Stock).Take(50).Explain();
        plan.Should().Contain("IndexPrefixSeek");
        plan.Should().NotContain("Sort");   // pre-ordered → no buffering sort
    }

    [Fact]
    public void Ascending_MatchesBruteForce()
    {
        var t = Seed();
        foreach (var cat in Cats)
        {
            var expected = All(t).Where(r => r.Value.Category == cat)
                .OrderBy(r => r.Value.Stock).ThenBy(r => r.Key).Select(r => r.Key).ToList();
            var actual = t.Query(p => p.Category).Equal(cat).OrderBy(p => p.Stock).ToList();

            actual.Select(r => r.Key).Should().Equal(expected, $"asc cat={cat}");
            actual.Should().OnlyContain(r => r.Value.Category == cat);
            for (var i = 1; i < actual.Count; i++)
                actual[i].Value.Stock.Should().BeGreaterThanOrEqualTo(actual[i - 1].Value.Stock);
        }
    }

    [Fact]
    public void Descending_MatchesBruteForce()
    {
        var t = Seed();
        foreach (var cat in Cats)
        {
            var expected = All(t).Where(r => r.Value.Category == cat)
                .OrderByDescending(r => r.Value.Stock).ThenByDescending(r => r.Key).Select(r => r.Key).ToList();
            var actual = t.Query(p => p.Category).Equal(cat).OrderByDescending(p => p.Stock).ToList();

            actual.Select(r => r.Key).Should().Equal(expected, $"desc cat={cat}");
            for (var i = 1; i < actual.Count; i++)
                actual[i].Value.Stock.Should().BeLessThanOrEqualTo(actual[i - 1].Value.Stock);
        }
    }

    [Fact]
    public void Take_ReturnsTopK_InOrder()
    {
        var t = Seed();
        var expected = All(t).Where(r => r.Value.Category == "charlie")
            .OrderBy(r => r.Value.Stock).ThenBy(r => r.Key).Take(37).Select(r => r.Key).ToList();
        var actual = t.Query(p => p.Category).Equal("charlie").OrderBy(p => p.Stock).Take(37).ToList();
        actual.Select(r => r.Key).Should().Equal(expected);
        actual.Count.Should().Be(37);
    }

    [Fact]
    public void Skip_AndTake_MatchBruteForce()
    {
        var t = Seed();
        var expected = All(t).Where(r => r.Value.Category == "delta")
            .OrderBy(r => r.Value.Stock).ThenBy(r => r.Key).Skip(20).Take(30).Select(r => r.Key).ToList();
        var actual = t.Query(p => p.Category).Equal("delta").OrderBy(p => p.Stock).Skip(20).Take(30).ToList();
        actual.Select(r => r.Key).Should().Equal(expected);
    }

    [Fact]
    public void NoMatch_ReturnsEmpty()
    {
        var t = Seed();
        t.Query(p => p.Category).Equal("nonexistent").OrderBy(p => p.Stock).Take(10).ToList()
            .Should().BeEmpty();
    }

    [Fact]
    public void Count_OnPrefixOrderedQuery_StillCorrect()
    {
        var t = Seed();
        foreach (var cat in Cats)
            t.Query(p => p.Category).Equal(cat).OrderBy(p => p.Stock).Count()
                .Should().Be(All(t).Count(r => r.Value.Category == cat), $"count cat={cat}");
    }

    [Fact]
    public void OrderByNonComposite_FallsBack_StillCorrect()
    {
        // No (Category,Price) index → must fall back to the normal plan and still be correct.
        var t = Seed();
        var expected = All(t).Where(r => r.Value.Category == "alpha")
            .OrderBy(r => r.Value.Price).Take(25).Select(r => r.Key).ToList();
        var actual = t.Query(p => p.Category).Equal("alpha").OrderBy(p => p.Price).Take(25).ToList();
        actual.Select(r => r.Key).Should().Equal(expected);
    }

    [Fact]
    public void Reflects_Updates_AndDeletes()
    {
        var t = Seed();
        t.Delete(1); t.Delete(5); // both "bravo"(i%4==1) and "bravo"(5%4==1)
        t[9] = new Product { Category = "bravo", Stock = -1, Price = 0 }; // becomes the new minimum-Stock bravo
        _engine.Commit();

        var expected = All(t).Where(r => r.Value.Category == "bravo")
            .OrderBy(r => r.Value.Stock).ThenBy(r => r.Key).Select(r => r.Key).ToList();
        var actual = t.Query(p => p.Category).Equal("bravo").OrderBy(p => p.Stock).ToList();
        actual.Select(r => r.Key).Should().Equal(expected);
        actual[0].Key.Should().Be(9); // the Stock=-1 row sorts first
    }
}
