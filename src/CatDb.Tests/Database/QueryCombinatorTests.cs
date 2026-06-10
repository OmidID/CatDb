// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// The fluent And/Or/Not/GroupOp combinators over the engine plan/execute path — boolean trees,
/// negation, and grouping, validated against LINQ.
/// </summary>
public class QueryCombinatorTests : IDisposable
{
    private readonly IStorageEngine _engine = CatDb.Database.CatDb.FromMemory();
    public void Dispose() => _engine.Dispose();

    public class Customer
    {
        public string City { get; set; } = "";
        public int Age { get; set; }
        public string Name { get; set; } = "";
    }

    private (ITable<int, Customer> Table, List<KeyValuePair<int, Customer>> All) Seed()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);
        table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);
        var cities = new[] { "berlin", "london", "nyc", "paris" };
        var all = new List<KeyValuePair<int, Customer>>();
        for (var i = 0; i < 400; i++)
        {
            var c = new Customer { City = cities[i % 4], Age = i % 20, Name = $"n{i % 5}" };
            table.Replace(i, c);
            all.Add(new(i, c));
        }
        _engine.Commit();
        return (table, all);
    }

    private static List<int> Sorted(IEnumerable<KeyValuePair<int, Customer>> rows)
        => rows.Select(kv => kv.Key).OrderBy(k => k).ToList();

    [Fact]
    public void Or_AcrossFields_Unions()
    {
        var (table, all) = Seed();
        var got = Sorted(table.Query(q => q.City).Equal("nyc").Or(q => q.City).Equal("berlin"));
        var exp = all.Where(kv => kv.Value.City is "nyc" or "berlin").Select(kv => kv.Key).OrderBy(k => k).ToList();
        got.Should().Equal(exp);
    }

    [Fact]
    public void NotEqual_NegatesPredicate()
    {
        var (table, all) = Seed();
        var got = Sorted(table.Query(q => q.City).NotEqual("nyc").And(q => q.Age).AtLeast(15));
        var exp = all.Where(kv => kv.Value.City != "nyc" && kv.Value.Age >= 15).Select(kv => kv.Key).OrderBy(k => k).ToList();
        got.Should().Equal(exp);
    }

    [Fact]
    public void NotBetween_NegatesRange()
    {
        var (table, all) = Seed();
        var got = Sorted(table.Query(q => q.City).Equal("london").And(q => q.Age).NotBetween(5, 15));
        var exp = all.Where(kv => kv.Value.City == "london" && !(kv.Value.Age >= 5 && kv.Value.Age <= 15))
                     .Select(kv => kv.Key).OrderBy(k => k).ToList();
        got.Should().Equal(exp);
    }

    [Fact]
    public void GroupOp_OrGroup_AndPredicate()
    {
        var (table, all) = Seed();
        // (City='nyc' OR City='london') AND Age >= 10
        var got = Sorted(table.Query()
            .GroupOp(g => g.And(f => f.City).Equal("nyc").Or(f => f.City).Equal("london"))
            .And(q => q.Age).AtLeast(10));
        var exp = all.Where(kv => (kv.Value.City is "nyc" or "london") && kv.Value.Age >= 10)
                     .Select(kv => kv.Key).OrderBy(k => k).ToList();
        got.Should().Equal(exp);
    }

    [Fact]
    public void PlanCache_ReusesPlan_AcrossDifferentValues()
    {
        var (table, _) = Seed();

        // Same shape, different literals → identical EXPLAIN (one cached plan serves both).
        var planA = table.Query(q => q.City).Equal("nyc").And(q => q.Age).AtLeast(5).Explain();
        var planB = table.Query(q => q.City).Equal("paris").And(q => q.Age).AtLeast(99).Explain();
        planA.Should().Be(planB);
        planA.Should().Contain("Intersect (AND)");

        // Results still differ by value (nyc rows have even ages: 2,6,10,…).
        table.Query(q => q.City).Equal("nyc").And(q => q.Age).Equal(2).Count()
             .Should().BeGreaterThan(0);
        table.Query(q => q.City).Equal("nonexistent").And(q => q.Age).Equal(2).Count()
             .Should().Be(0);
    }
}
