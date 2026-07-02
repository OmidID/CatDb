// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;
using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Database.Querying;
using CatDb.Extensions;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// Exercises the engine-level structured query executor directly (no fluent sugar): multi-index
/// AND intersection, structured residual on non-indexed fields, and engine-side ordering.
/// </summary>
public class EngineQueryExecutorTests : IDisposable
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

    private (ITable<int, Customer> Table, ITableIndexManager Idx, List<KeyValuePair<int, Customer>> All) Seed()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);
        table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);

        var cities = new[] { "berlin", "london", "nyc" };
        var all = new List<KeyValuePair<int, Customer>>();
        for (var i = 0; i < 300; i++)
        {
            var c = new Customer { Email = $"u{i:D4}@x.com", City = cities[i % 3], Age = i % 20, Name = $"n{i % 5}" };
            table.Replace(i, c);
            all.Add(new(i, c));
        }
        _engine.Commit();
        return (table, table.Indexes, all);
    }

    private static FilterNode Eq<T>(string member, T value) =>
        FilterNode.Leaf(new FieldFilter { Member = member, Op = FilterOp.Equal, FieldType = typeof(T), Value = value });

    private static FilterNode Leaf<T>(string member, FilterOp op, T value) =>
        FilterNode.Leaf(new FieldFilter { Member = member, Op = op, FieldType = typeof(T), Value = value });

    // No ORDER BY => engine returns rows in (unspecified) plan order; sort here to compare as a set.
    private List<int> Keys(ITableIndexManager idx, EngineQuery q) =>
        idx.ExecuteQuery(q).Select(kv => (int)kv.Key).OrderBy(k => k).ToList();

    [Fact]
    public void TwoIndexes_AndIntersection_MatchesLinq()
    {
        var (_, idx, all) = Seed();

        var q = new EngineQuery { Filter = FilterNode.And(Eq("City", "nyc"), Leaf("Age", FilterOp.AtLeast, 10)) };

        var got = Keys(idx, q);
        var expected = all.Where(kv => kv.Value.City == "nyc" && kv.Value.Age >= 10)
                          .Select(kv => kv.Key).OrderBy(k => k).ToList();

        got.Should().Equal(expected);
        got.Should().NotBeEmpty();
    }

    [Fact]
    public void ThreeWay_Intersection_TwoIndexed_OneResidual()
    {
        var (_, idx, all) = Seed();

        var between = FilterNode.Leaf(new FieldFilter { Member = "Age", Op = FilterOp.Between, FieldType = typeof(int),
            Value = 5, Value2 = 15 });
        var q = new EngineQuery { Filter = FilterNode.All([Eq("City", "london"), between, Eq("Name", "n2")]) };

        var got = Keys(idx, q);
        var expected = all.Where(kv => kv.Value.City == "london"
                                       && kv.Value.Age >= 5 && kv.Value.Age <= 15
                                       && kv.Value.Name == "n2")
                          .Select(kv => kv.Key).OrderBy(k => k).ToList();

        got.Should().Equal(expected);
    }

    [Fact]
    public void Or_OfTwoIndexes_UnionsMatchesLinq()
    {
        var (_, idx, all) = Seed();

        // (City = 'nyc' OR City = 'berlin') AND Age >= 10
        var cityOr = FilterNode.Or(Eq("City", "nyc"), Eq("City", "berlin"));
        var q = new EngineQuery { Filter = FilterNode.And(cityOr, Leaf("Age", FilterOp.AtLeast, 10)) };

        var got = Keys(idx, q);
        var expected = all.Where(kv => (kv.Value.City == "nyc" || kv.Value.City == "berlin") && kv.Value.Age >= 10)
                          .Select(kv => kv.Key).OrderBy(k => k).ToList();
        got.Should().Equal(expected);
        got.Should().NotBeEmpty();
    }

    [Fact]
    public void Sort_ByField_Descending_ThenTake()
    {
        var (_, idx, all) = Seed();

        var q = new EngineQuery { Take = 5, Filter = Eq("City", "nyc") };
        q.Sorts.Add(new SortField { Member = "Age", FieldType = typeof(int), Descending = true });

        var rows = idx.ExecuteQuery(q).Select(kv => (int)kv.Key).ToList();

        var expected = all.Where(kv => kv.Value.City == "nyc")
                          .OrderByDescending(kv => kv.Value.Age).ThenByDescending(kv => kv.Key)
                          .Select(kv => kv.Key).Take(5).ToList();

        rows.Should().Equal(expected);
    }

    [Fact]
    public void NoIndexPredicate_FallsBackToScan_WithResidual()
    {
        var (_, idx, all) = Seed();

        var q = new EngineQuery { Filter = Eq("Name", "n3") };   // no index on Name → scan + residual

        var got = Keys(idx, q);
        var expected = all.Where(kv => kv.Value.Name == "n3").Select(kv => kv.Key).OrderBy(k => k).ToList();
        got.Should().Equal(expected);
    }
}
