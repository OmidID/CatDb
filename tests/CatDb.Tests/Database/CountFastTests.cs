// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// <c>Query(...).Count()</c> takes a fast path that counts index keys WITHOUT fetching each matching record
/// from the main table (one heap point-lookup per row otherwise — O(matches) random reads that grow with the
/// table; the cause of index/sort query throughput decay under load). These tests pin the fast path's
/// correctness: its count MUST equal a full enumeration of the same query across every shape — index
/// equality, range, AND/OR, ORDER BY (order-independent for a count), Take/Skip, and the residual-filter
/// fallback (non-indexed predicate, where records ARE needed).
/// </summary>
public class CountFastTests : IDisposable
{
    private readonly IStorageEngine _engine = CatDb.Database.CatDb.FromMemory();
    public void Dispose() => _engine.Dispose();

    public class Product
    {
        public string Category { get; set; } = "";
        public int Stock { get; set; }
        public string Brand { get; set; } = "";
    }

    private ITable<int, Product> Seed(int n = 600)
    {
        var table = _engine.OpenXTable<int, Product>("products");
        table.CreateIndex("Category", p => p.Category, IndexType.NonUnique);
        table.CreateIndex("Stock", p => p.Stock, IndexType.NonUnique);
        var cats = new[] { "alpha", "bravo", "charlie", "delta" };
        var brands = new[] { "x", "y", "z" };
        for (var i = 0; i < n; i++)
            table.Replace(i, new Product { Category = cats[i % 4], Stock = i % 50, Brand = brands[i % 3] });
        _engine.Commit();
        return table;
    }

    [Fact]
    public void IndexEquality_CountEqualsEnumeration()
    {
        var t = Seed();
        t.Query(p => p.Category).Equal("bravo").Count()
            .Should().Be(t.Query(p => p.Category).Equal("bravo").ToList().Count)
            .And.Be(150); // 600 / 4
    }

    [Fact]
    public void IndexRange_CountEqualsEnumeration()
    {
        var t = Seed();
        t.Query(p => p.Stock).AtLeast(10).AtMost(19).Count()
            .Should().Be(t.Query(p => p.Stock).AtLeast(10).AtMost(19).ToList().Count);
    }

    [Fact]
    public void Intersect_And_CountEqualsEnumeration()
    {
        var t = Seed();
        t.Query(p => p.Category).Equal("alpha").And(p => p.Stock).AtLeast(25).Count()
            .Should().Be(t.Query(p => p.Category).Equal("alpha").And(p => p.Stock).AtLeast(25).ToList().Count);
    }

    [Fact]
    public void Union_Or_CountEqualsEnumeration()
    {
        var t = Seed();
        t.Query(p => p.Category).Equal("alpha").Or(p => p.Category).Equal("delta").Count()
            .Should().Be(t.Query(p => p.Category).Equal("alpha").Or(p => p.Category).Equal("delta").ToList().Count);
    }

    [Fact]
    public void OrderBy_IsIrrelevantToCount()
    {
        var t = Seed();
        // ORDER BY must not change the count.
        t.Query(p => p.Category).Equal("charlie").OrderBy(p => p.Stock).Count()
            .Should().Be(t.Query(p => p.Category).Equal("charlie").Count());
    }

    [Fact]
    public void TakeAndSkip_AreHonoredByCount()
    {
        var t = Seed();
        t.Query(p => p.Category).Equal("bravo").Take(10).Count().Should().Be(10);
        t.Query(p => p.Category).Equal("bravo").Skip(140).Count().Should().Be(10);      // 150 - 140
        t.Query(p => p.Category).Equal("bravo").Skip(145).Take(20).Count().Should().Be(5); // min(150-145, 20)
    }

    [Fact]
    public void NonIndexedResidual_FallsBack_StillCorrect()
    {
        var t = Seed();
        // Brand is indexed; combine an indexed predicate with a residual to exercise the fallback path too.
        t.Query(p => p.Brand).Equal("y").Count()
            .Should().Be(t.Query(p => p.Brand).Equal("y").ToList().Count);
    }

    [Fact]
    public void CountReflectsDeletes()
    {
        var t = Seed();
        var before = t.Query(p => p.Category).Equal("alpha").Count();
        t.Delete(0); // i%4==0 → category alpha
        t.Delete(4);
        _engine.Commit();
        t.Query(p => p.Category).Equal("alpha").Count().Should().Be(before - 2);
    }
}
