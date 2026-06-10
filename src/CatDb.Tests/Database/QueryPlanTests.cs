// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// EXPLAIN-level assertions on the physical plan the engine chooses — proves filters use index seeks
/// (not table scans), multi-index AND intersects, OR unions, and bounded Top-K sort kicks in with Take.
/// </summary>
public class QueryPlanTests : IDisposable
{
    private readonly IStorageEngine _engine = CatDb.Database.CatDb.FromMemory();
    public void Dispose() => _engine.Dispose();

    public class Customer
    {
        public string City { get; set; } = "";
        public int Age { get; set; }
        public string Name { get; set; } = "";
    }

    private ITable<int, Customer> Seed()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);
        table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);
        var cities = new[] { "berlin", "london", "nyc" };
        for (var i = 0; i < 300; i++)
            table.Replace(i, new Customer { City = cities[i % 3], Age = i % 20, Name = $"n{i % 5}" });
        _engine.Commit();
        return table;
    }

    [Fact]
    public void SingleIndexedEquality_UsesIndexSeek_NotScan()
    {
        var plan = Seed().Query(q => q.City).Equal("nyc").Explain();
        plan.Should().Contain("IndexEqualSeek");
        plan.Should().NotContain("TableScan");
    }

    [Fact]
    public void TwoIndexedPredicates_Intersect()
    {
        var plan = Seed().Query(q => q.City).Equal("nyc").And(q => q.Age).AtLeast(10).Explain();
        plan.Should().Contain("Intersect (AND)");
        plan.Should().Contain("IndexEqualSeek");
        plan.Should().Contain("IndexRangeSeek");
    }

    [Fact]
    public void Or_OfIndexedPredicates_Unions()
    {
        var plan = Seed().Query(q => q.City).Equal("nyc").Or(q => q.City).Equal("berlin").Explain();
        plan.Should().Contain("Union (OR)");
    }

    [Fact]
    public void NonIndexedPredicate_FallsBackToScan_WithFilter()
    {
        var plan = Seed().Query(q => q.Name).Equal("n3").Explain();
        plan.Should().Contain("TableScan");
        plan.Should().Contain("Filter");
    }

    [Fact]
    public void FilterPlusSortWithTake_UsesBoundedTopK()
    {
        var plan = Seed().Query(q => q.City).Equal("nyc").OrderBy(o => o.Age).Take(5).Explain();
        plan.Should().Contain("IndexEqualSeek");
        plan.Should().Contain("Sort (Top-5)");
    }

    [Fact]
    public void OrderByKeyOnly_StreamsScan_NoSort()
    {
        var plan = Seed().Query().OrderByKeyDescending().Take(10).Explain();
        plan.Should().Contain("TableScan (backward)");
        plan.Should().NotContain("Sort");
    }

    [Fact]
    public void GroupedOr_AndIndexed_PlansCorrectly()
    {
        // (City='nyc' OR City='london') AND Age >= 5  → Union under Intersect.
        var plan = Seed().Query()
            .GroupOp(g => g.And(f => f.City).Equal("nyc").Or(f => f.City).Equal("london"))
            .And(q => q.Age).AtLeast(5)
            .Explain();
        plan.Should().Contain("Union (OR)");
        plan.Should().Contain("Intersect (AND)");
    }
}
