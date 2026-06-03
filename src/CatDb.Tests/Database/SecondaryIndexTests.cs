// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;
using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using FluentAssertions;

namespace CatDb.Tests.Database;

public class SecondaryIndexTests : IDisposable
{
    private readonly IStorageEngine _engine;

    public class Customer
    {
        public string Email { get; set; } = "";
        public string City { get; set; } = "";
        public int Age { get; set; }
        public string Name { get; set; } = "";
    }

    public SecondaryIndexTests()
    {
        _engine = CatDb.Database.CatDb.FromMemory();
    }

    public void Dispose() => _engine.Dispose();

    [Fact]
    public void UniqueIndex_Find_ReturnsCorrectRecord()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);

        table.Replace(1, new Customer { Email = "a@b.com", City = "NYC", Age = 30, Name = "Alice" });
        table.Replace(2, new Customer { Email = "x@y.com", City = "LA", Age = 25, Name = "Bob" });
        _engine.Commit();

        var results = table.FindByIndex<int, Customer, string>("Email", "a@b.com").ToList();
        results.Should().HaveCount(1);
        results[0].Key.Should().Be(1);
        results[0].Value.Name.Should().Be("Alice");
    }

    [Fact]
    public void NonUniqueIndex_Find_ReturnsMultipleRecords()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        table.Replace(1, new Customer { Email = "a@b.com", City = "NYC", Age = 30, Name = "Alice" });
        table.Replace(2, new Customer { Email = "x@y.com", City = "NYC", Age = 25, Name = "Bob" });
        table.Replace(3, new Customer { Email = "c@d.com", City = "LA", Age = 35, Name = "Carol" });
        _engine.Commit();

        var results = table.FindByIndex<int, Customer, string>("City", "NYC").ToList();
        results.Should().HaveCount(2);
        results.Select(r => r.Value.Name).OrderBy(n => n).Should().BeEquivalentTo(new[] { "Alice", "Bob" });
    }

    [Fact]
    public void UniqueIndex_ThrowsOnDuplicate()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);

        table.Replace(1, new Customer { Email = "dup@test.com", City = "NYC" });

        var act = () => table.Replace(2, new Customer { Email = "dup@test.com", City = "LA" });
        act.Should().Throw<UniqueIndexViolationException>();
    }

    [Fact]
    public void UniqueIndex_AllowsSameKeyUpdate()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);

        table.Replace(1, new Customer { Email = "a@b.com", City = "NYC" });

        var act = () => table.Replace(1, new Customer { Email = "a@b.com", City = "LA" });
        act.Should().NotThrow();
    }

    [Fact]
    public void UniqueIndex_Update_RemovesStaleEntry()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);

        table.Replace(1, new Customer { Email = "old@test.com", City = "NYC" });
        table.Replace(1, new Customer { Email = "new@test.com", City = "NYC" });
        _engine.Commit();

        table.ExistsInIndex<int, Customer, string>("Email", "old@test.com").Should().BeFalse();
        table.ExistsInIndex<int, Customer, string>("Email", "new@test.com").Should().BeTrue();
    }

    [Fact]
    public void NonUniqueIndex_Update_MaintainsCorrectly()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        table.Replace(1, new Customer { Email = "a@b.com", City = "NYC" });
        table.Replace(2, new Customer { Email = "x@y.com", City = "NYC" });
        _engine.Commit();

        table.Replace(1, new Customer { Email = "a@b.com", City = "LA" });
        _engine.Commit();

        table.FindByIndex<int, Customer, string>("City", "NYC").Should().HaveCount(1);
        table.FindByIndex<int, Customer, string>("City", "LA").Should().HaveCount(1);
    }

    [Fact]
    public void Delete_RemovesFromIndex()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);

        table.Replace(1, new Customer { Email = "del@test.com", City = "NYC" });
        _engine.Commit();
        table.ExistsInIndex<int, Customer, string>("Email", "del@test.com").Should().BeTrue();

        table.Delete(1);
        _engine.Commit();
        table.ExistsInIndex<int, Customer, string>("Email", "del@test.com").Should().BeFalse();
    }

    [Fact]
    public void DeleteRange_RemovesFromIndex()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        for (int i = 1; i <= 10; i++)
            table.Replace(i, new Customer { Email = $"u{i}@t.com", City = "NYC" });
        _engine.Commit();

        table.CountByIndex<int, Customer, string>("City", "NYC").Should().Be(10);

        table.Delete(3, 7);
        _engine.Commit();

        table.CountByIndex<int, Customer, string>("City", "NYC").Should().Be(5);
    }

    [Fact]
    public void Clear_ClearsIndexes()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);

        table.Replace(1, new Customer { Email = "a@b.com" });
        table.Replace(2, new Customer { Email = "x@y.com" });
        _engine.Commit();

        table.Clear();
        _engine.Commit();

        table.ExistsInIndex<int, Customer, string>("Email", "a@b.com").Should().BeFalse();
        table.ExistsInIndex<int, Customer, string>("Email", "x@y.com").Should().BeFalse();
    }

    [Fact]
    public void InsertOrIgnore_MaintainsIndex()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);

        table.InsertOrIgnore(1, new Customer { Email = "a@b.com", City = "NYC" });
        _engine.Commit();
        table.ExistsInIndex<int, Customer, string>("Email", "a@b.com").Should().BeTrue();

        table.InsertOrIgnore(1, new Customer { Email = "other@b.com", City = "LA" });
        _engine.Commit();
        table.ExistsInIndex<int, Customer, string>("Email", "a@b.com").Should().BeTrue();
        table.ExistsInIndex<int, Customer, string>("Email", "other@b.com").Should().BeFalse();
    }

    [Fact]
    public void UniqueIndex_RangeSearch()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);

        table.Replace(1, new Customer { Email = "alice@test.com" });
        table.Replace(2, new Customer { Email = "bob@test.com" });
        table.Replace(3, new Customer { Email = "carol@test.com" });
        table.Replace(4, new Customer { Email = "dave@test.com" });
        _engine.Commit();

        var results = table.FindByIndexRange<int, Customer, string>(
            "Email", "bob@test.com", true, "dave@test.com", true).ToList();
        results.Should().HaveCount(3); // bob, carol, dave
    }

    [Fact]
    public void MultipleIndexes_AllMaintained()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        table.Replace(1, new Customer { Email = "a@b.com", City = "NYC" });
        table.Replace(2, new Customer { Email = "x@y.com", City = "NYC" });
        _engine.Commit();

        table.ExistsInIndex<int, Customer, string>("Email", "a@b.com").Should().BeTrue();
        table.CountByIndex<int, Customer, string>("City", "NYC").Should().Be(2);

        table.Delete(1);
        _engine.Commit();

        table.ExistsInIndex<int, Customer, string>("Email", "a@b.com").Should().BeFalse();
        table.CountByIndex<int, Customer, string>("City", "NYC").Should().Be(1);
    }

    [Fact]
    public void CompositeIndex_ByMemberNames()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("CityAge", new[] { "City", "Age" }, IndexType.NonUnique);

        table.Replace(1, new Customer { Email = "a@b.com", City = "NYC", Age = 30 });
        table.Replace(2, new Customer { Email = "x@y.com", City = "NYC", Age = 25 });
        table.Replace(3, new Customer { Email = "c@d.com", City = "LA", Age = 30 });
        _engine.Commit();

        // Search by composite value - NYC, Age=30
        var fieldValue = new Data<Slots<string, int>>(new Slots<string, int>("NYC", 30));
        var results = table.Indexes.FindByIndex("CityAge", fieldValue).ToList();
        results.Should().HaveCount(1);
        ((Data<int>)results[0].Key).Value.Should().Be(1);
    }

    [Fact]
    public void CreateIndex_BySlotIndices()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        // City is slot 1 (Email=0, City=1, Age=2, Name=3)
        table.Indexes.CreateIndex("SlotCity", new[] { 1 }, IndexType.NonUnique);

        table.Replace(1, new Customer { Email = "a@b.com", City = "NYC", Age = 30, Name = "Alice" });
        table.Replace(2, new Customer { Email = "x@y.com", City = "LA", Age = 25, Name = "Bob" });
        _engine.Commit();

        var fieldValue = new Data<string>("NYC");
        var results = table.Indexes.FindByIndex("SlotCity", fieldValue).ToList();
        results.Should().HaveCount(1);
    }

    [Fact]
    public void RebuildIndex_ReconstructsFromData()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");

        for (int i = 1; i <= 50; i++)
            table.Replace(i, new Customer { Email = $"u{i}@t.com", City = i % 2 == 0 ? "NYC" : "LA" });
        _engine.Commit();

        table.CreateIndex("City", c => c.City, IndexType.NonUnique);
        table.RebuildIndex("City");

        table.CountByIndex<int, Customer, string>("City", "NYC").Should().Be(25);
        table.CountByIndex<int, Customer, string>("City", "LA").Should().Be(25);
    }

    [Fact]
    public void DropIndex_RemovesIndex()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);
        table.Replace(1, new Customer { Email = "a@b.com" });
        _engine.Commit();

        table.DropIndex("Email");

        var act = () => table.ExistsInIndex<int, Customer, string>("Email", "a@b.com");
        act.Should().Throw<KeyNotFoundException>();
    }

    [Fact]
    public void ListIndexes_ReturnsAll()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        var indexes = table.Indexes.ListIndexes();
        indexes.Should().HaveCount(2);
        indexes.Select(i => i.Name).OrderBy(n => n).Should().BeEquivalentTo(new[] { "City", "Email" });
    }

    [Fact]
    public void LargeDataset_10K_Records()
    {
        var table = _engine.OpenXTable<long, Customer>("large");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        var cities = new[] { "NYC", "London", "Tokyo", "Paris", "Berlin" };
        for (long i = 1; i <= 10_000; i++)
        {
            table.Replace(i, new Customer
            {
                Email = $"user{i}@test.com",
                City = cities[i % 5],
                Age = (int)(20 + i % 50),
                Name = $"User{i}",
            });
        }
        _engine.Commit();

        var found = table.FindByIndex<long, Customer, string>("Email", "user5000@test.com").ToList();
        found.Should().HaveCount(1);
        found[0].Key.Should().Be(5000);

        table.CountByIndex<long, Customer, string>("City", "NYC").Should().Be(2000);
        table.CountByIndex<long, Customer, string>("City", "Tokyo").Should().Be(2000);
    }

    [Fact]
    public void ManualIndex_UsingMemberNames()
    {
        var table = _engine.OpenXTable<int, Customer>("manual_test");
        table.Indexes.CreateIndex("ManualCity", new[] { "City" }, IndexType.NonUnique);

        table.Replace(1, new Customer { City = "NYC" });
        table.Replace(2, new Customer { City = "NYC" });
        table.Replace(3, new Customer { City = "LA" });
        _engine.Commit();

        var fieldValue = new Data<string>("NYC");
        var results = table.Indexes.FindByIndex("ManualCity", fieldValue).ToList();
        results.Should().HaveCount(2);
    }

    [Fact]
    public void NoIndexes_ZeroOverhead()
    {
        var table = _engine.OpenXTable<int, Customer>("fast");

        for (int i = 1; i <= 1000; i++)
            table.Replace(i, new Customer { Email = $"u{i}@t.com", City = "NYC" });
        _engine.Commit();

        table.Count().Should().Be(1000);
        table.TryGet(500, out var rec).Should().BeTrue();
        rec!.Email.Should().Be("u500@t.com");
    }

    // ── OrderBy / OrderByDescending on IndexQuery ───────────────────────────

    [Fact]
    public void IndexQuery_OrderBy_SortsAscending()
    {
        var table = _engine.OpenXTable<int, Customer>("order_customers");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        table.Replace(1, new Customer { Email = "a@b.com", City = "NYC", Age = 30, Name = "Charlie" });
        table.Replace(2, new Customer { Email = "b@b.com", City = "NYC", Age = 25, Name = "Alice" });
        table.Replace(3, new Customer { Email = "c@b.com", City = "NYC", Age = 35, Name = "Bob" });
        table.Replace(4, new Customer { Email = "d@b.com", City = "LA",  Age = 20, Name = "Dave" });
        _engine.Commit();

        var results = table.Query(c => c.City).Equals("NYC").OrderBy(c => c.Name).ToList();
        results.Select(r => r.Value.Name).Should().Equal("Alice", "Bob", "Charlie");
    }

    [Fact]
    public void IndexQuery_OrderByDescending_SortsDescending()
    {
        var table = _engine.OpenXTable<int, Customer>("order_desc_customers");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        table.Replace(1, new Customer { Email = "a@b.com", City = "NYC", Age = 30, Name = "Charlie" });
        table.Replace(2, new Customer { Email = "b@b.com", City = "NYC", Age = 25, Name = "Alice" });
        table.Replace(3, new Customer { Email = "c@b.com", City = "NYC", Age = 35, Name = "Bob" });
        _engine.Commit();

        var results = table.Query(c => c.City).Equals("NYC").OrderByDescending(c => c.Age).ToList();
        results.Select(r => r.Value.Name).Should().Equal("Bob", "Charlie", "Alice");
    }

    [Fact]
    public void IndexQuery_OrderBy_ThenBy_MultipleSortKeys()
    {
        var table = _engine.OpenXTable<int, Customer>("multi_sort_customers");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        table.Replace(1, new Customer { Email = "a@b.com", City = "NYC", Age = 30, Name = "Bob" });
        table.Replace(2, new Customer { Email = "b@b.com", City = "NYC", Age = 25, Name = "Alice" });
        table.Replace(3, new Customer { Email = "c@b.com", City = "NYC", Age = 30, Name = "Alice" });
        table.Replace(4, new Customer { Email = "d@b.com", City = "NYC", Age = 25, Name = "Bob" });
        _engine.Commit();

        // OrderBy Age ascending, then Name ascending
        var results = table.Query(c => c.City)
            .Equals("NYC")
            .OrderBy(c => c.Age)
            .OrderBy(c => c.Name)
            .ToList();

        results.Select(r => new { r.Value.Age, r.Value.Name })
            .Should().Equal(
                new { Age = 25, Name = "Alice" },
                new { Age = 25, Name = "Bob" },
                new { Age = 30, Name = "Alice" },
                new { Age = 30, Name = "Bob" }
            );
    }

    [Fact]
    public void IndexQuery_OrderBy_WithTake_LimitsAfterSort()
    {
        var table = _engine.OpenXTable<int, Customer>("sort_take_customers");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        table.Replace(1, new Customer { Email = "a@b.com", City = "NYC", Age = 30, Name = "Charlie" });
        table.Replace(2, new Customer { Email = "b@b.com", City = "NYC", Age = 25, Name = "Alice" });
        table.Replace(3, new Customer { Email = "c@b.com", City = "NYC", Age = 35, Name = "Bob" });
        table.Replace(4, new Customer { Email = "d@b.com", City = "NYC", Age = 20, Name = "Dave" });
        _engine.Commit();

        var results = table.Query(c => c.City)
            .Equals("NYC")
            .OrderBy(c => c.Age)
            .Take(2)
            .ToList();

        results.Select(r => r.Value.Name).Should().Equal("Dave", "Alice");
    }

    [Fact]
    public void IndexQuery_OrderBy_RangeSearch_SortsFilteredResults()
    {
        var table = _engine.OpenXTable<int, Customer>("range_sort_customers");
        // Use unique index since all ages are unique in this test data
        table.CreateIndex("Age", c => c.Age, IndexType.Unique);

        table.Replace(1, new Customer { Email = "a@b.com", City = "NYC", Age = 20, Name = "Charlie" });
        table.Replace(2, new Customer { Email = "b@b.com", City = "NYC", Age = 25, Name = "Alice" });
        table.Replace(3, new Customer { Email = "c@b.com", City = "NYC", Age = 30, Name = "Bob" });
        table.Replace(4, new Customer { Email = "d@b.com", City = "NYC", Age = 35, Name = "Dave" });
        table.Replace(5, new Customer { Email = "e@b.com", City = "NYC", Age = 40, Name = "Eve" });
        _engine.Commit();

        var results = table.Query(c => c.Age)
            .AtLeast(25)
            .AtMost(35)
            .OrderByDescending(c => c.Name)
            .ToList();

        results.Select(r => r.Value.Name).Should().Equal("Dave", "Bob", "Alice");
    }

    [Fact]
    public void IndexQuery_OrderBy_NoResults_ReturnsEmpty()
    {
        var table = _engine.OpenXTable<int, Customer>("empty_sort_customers");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        table.Replace(1, new Customer { Email = "a@b.com", City = "NYC", Name = "Alice" });
        _engine.Commit();

        var results = table.Query(c => c.City)
            .Equals("LA")
            .OrderBy(c => c.Name)
            .ToList();

        results.Should().BeEmpty();
    }

    [Fact]
    public void IndexQuery_NoSort_StaysLazy()
    {
        var table = _engine.OpenXTable<int, Customer>("lazy_customers");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        table.Replace(1, new Customer { Email = "a@b.com", City = "NYC", Name = "Alice" });
        table.Replace(2, new Customer { Email = "b@b.com", City = "NYC", Name = "Bob" });
        _engine.Commit();

        // Without OrderBy, Take should short-circuit during enumeration
        var results = table.Query(c => c.City).Equals("NYC").Take(1).ToList();
        results.Should().HaveCount(1);
    }
}
