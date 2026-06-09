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

    private static FieldFilter Eq<T>(string member, T value) =>
        new() { Member = member, Op = FilterOp.Equal, FieldType = typeof(T), Value = new Data<T>(value) };

    private List<int> Keys(ITableIndexManager idx, EngineQuery q) =>
        idx.ExecuteQuery(q).Select(kv => ((Data<int>)kv.Key).Value).ToList();

    [Fact]
    public void TwoIndexes_AndIntersection_MatchesLinq()
    {
        var (_, idx, all) = Seed();

        var q = new EngineQuery();
        q.Filters.Add(Eq("City", "nyc"));
        q.Filters.Add(new FieldFilter { Member = "Age", Op = FilterOp.AtLeast, FieldType = typeof(int), Value = new Data<int>(10) });

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

        var q = new EngineQuery();
        q.Filters.Add(Eq("City", "london"));                                  // indexed
        q.Filters.Add(new FieldFilter { Member = "Age", Op = FilterOp.Between, FieldType = typeof(int),
            Value = new Data<int>(5), Value2 = new Data<int>(15) });          // indexed (range)
        q.Filters.Add(Eq("Name", "n2"));                                      // residual (no index)

        var got = Keys(idx, q);
        var expected = all.Where(kv => kv.Value.City == "london"
                                       && kv.Value.Age >= 5 && kv.Value.Age <= 15
                                       && kv.Value.Name == "n2")
                          .Select(kv => kv.Key).OrderBy(k => k).ToList();

        got.Should().Equal(expected);
    }

    [Fact]
    public void Sort_ByField_Descending_ThenTake()
    {
        var (_, idx, all) = Seed();

        var q = new EngineQuery { Take = 5 };
        q.Filters.Add(Eq("City", "nyc"));
        q.Sorts.Add(new SortField { Member = "Age", FieldType = typeof(int), Descending = true });

        var rows = idx.ExecuteQuery(q).Select(kv => ((Data<int>)kv.Key).Value).ToList();

        var expected = all.Where(kv => kv.Value.City == "nyc")
                          .OrderByDescending(kv => kv.Value.Age).ThenByDescending(kv => kv.Key)
                          .Select(kv => kv.Key).Take(5).ToList();

        rows.Should().Equal(expected);
    }

    [Fact]
    public void NoIndexPredicate_FallsBackToScan_WithResidual()
    {
        var (_, idx, all) = Seed();

        var q = new EngineQuery();
        q.Filters.Add(Eq("Name", "n3"));   // no index on Name → full scan + residual

        var got = Keys(idx, q);
        var expected = all.Where(kv => kv.Value.Name == "n3").Select(kv => kv.Key).OrderBy(k => k).ToList();
        got.Should().Equal(expected);
    }
}
