// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// The fluent Solution-B builder over the engine executor: chained field predicates across multiple
/// indexes (AND), residual fields, multi-key ordering, paging — all executed inside the engine.
/// (Uses the temporary entry name <c>.Query(...)</c>; renamed to <c>.Query(...)</c> after the old
/// surface is removed.)
/// </summary>
public class QueryBuilderTests : IDisposable
{
    private readonly IStorageEngine _engine = CatDb.Database.CatDb.FromMemory();
    public void Dispose() => _engine.Dispose();

    public class Customer
    {
        public string Email { get; set; } = "";
        public string City { get; set; } = "";
        public int Age { get; set; }
        public string Name { get; set; } = "";
    }

    private (ITable<int, Customer> Table, List<KeyValuePair<int, Customer>> All) Seed()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);
        table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);
        table.CreateIndex("Name", c => c.Name, IndexType.NonUnique);

        var cities = new[] { "berlin", "london", "nyc" };
        var all = new List<KeyValuePair<int, Customer>>();
        for (var i = 0; i < 400; i++)
        {
            var c = new Customer { Email = $"u{i:D4}@x.com", City = cities[i % 3], Age = i % 25, Name = $"n{i % 6}" };
            table.Replace(i, c);
            all.Add(new(i, c));
        }
        _engine.Commit();
        return (table, all);
    }

    [Fact]
    public void Headline_MultiField_MultiIndex_Filter_Then_MultiSort()
    {
        var (table, all) = Seed();

        var rows = table.Query(q => q.City).Equal("nyc")
            .Then(q => q.Age).AtLeast(5).AtMost(20)
            .Then(q => q.Name).Equal("n2")
            .OrderBy(o => o.Age).ThenByDescending(o => o.Email)
            .ToList();

        var expected = all
            .Where(kv => kv.Value.City == "nyc" && kv.Value.Age >= 5 && kv.Value.Age <= 20 && kv.Value.Name == "n2")
            .OrderBy(kv => kv.Value.Age).ThenByDescending(kv => kv.Value.Email, StringComparer.Ordinal)
            .Select(kv => kv.Key).ToList();

        rows.Select(r => r.Key).Should().Equal(expected);
        rows.Should().NotBeEmpty();
    }

    [Fact]
    public void TwoIndexes_Intersect()
    {
        var (table, all) = Seed();

        var keys = table.Query(q => q.City).Equal("london")
            .Then(q => q.Age).Equal(10)
            .Select(kv => kv.Key).OrderBy(k => k).ToList();

        var expected = all.Where(kv => kv.Value.City == "london" && kv.Value.Age == 10)
                          .Select(kv => kv.Key).OrderBy(k => k).ToList();
        keys.Should().Equal(expected);
    }

    [Fact]
    public void Take_And_Count()
    {
        var (table, all) = Seed();

        var expectedCount = all.Count(kv => kv.Value.City == "berlin" && kv.Value.Age >= 10);
        table.Query(q => q.City).Equal("berlin").Then(q => q.Age).AtLeast(10).Count()
             .Should().Be(expectedCount);

        var top = table.Query(q => q.City).Equal("berlin").OrderByDescending(o => o.Age).Take(5)
                       .Select(kv => kv.Value.Age).ToList();
        top.Should().BeInDescendingOrder();
        top.Should().HaveCount(5);
    }

    [Fact]
    public void OrderByKey_NoFilter()
    {
        var (table, _) = Seed();
        var keys = table.Query().OrderByKeyDescending().Take(4).Select(kv => kv.Key).ToList();
        keys.Should().Equal(399, 398, 397, 396);
    }

    [Fact]
    public void StartsWith_Prefix_Residual()
    {
        var (table, all) = Seed();
        var keys = table.Query(q => q.Email).StartsWith("u001")
            .Select(kv => kv.Key).OrderBy(k => k).ToList();
        var expected = all.Where(kv => kv.Value.Email.StartsWith("u001", StringComparison.Ordinal))
                          .Select(kv => kv.Key).OrderBy(k => k).ToList();
        keys.Should().Equal(expected);
    }
}
