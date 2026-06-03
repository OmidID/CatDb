// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using FluentAssertions;

namespace CatDb.Tests.Database;

public class OrderedQueryTests : IDisposable
{
    private readonly IStorageEngine _engine;

    public class Customer
    {
        public string Email { get; set; } = "";
        public string City { get; set; } = "";
        public int Age { get; set; }
        public string Name { get; set; } = "";
    }

    public OrderedQueryTests()
    {
        _engine = CatDb.Database.CatDb.FromMemory();
    }

    public void Dispose() => _engine.Dispose();

    private ITable<int, Customer> Seed()
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        // Names/ages chosen so primary (Name asc) and secondary (Age desc) ordering differ.
        table.Replace(1, new Customer { Email = "user11@example.com", City = "NYC", Age = 30, Name = "Carol" });
        table.Replace(2, new Customer { Email = "user12@example.com", City = "LA", Age = 25, Name = "Alice" });
        table.Replace(3, new Customer { Email = "user13@example.com", City = "NYC", Age = 40, Name = "Alice" });
        table.Replace(4, new Customer { Email = "user14@example.com", City = "LA", Age = 22, Name = "Bob" });
        table.Replace(5, new Customer { Email = "user99@example.com", City = "NYC", Age = 50, Name = "Zoe" });
        _engine.Commit();
        return table;
    }

    [Fact]
    public void IndexFilter_OrderBy_FieldAscending()
    {
        var table = Seed();

        var results = table
            .Query(c => c.Email)
            .AtLeast("user11@example.com")
            .AtMost("user14@example.com")
            .OrderBy(c => c.Name)
            .ToList();

        // user11..user14 → Carol, Alice, Alice, Bob → sorted by Name asc
        results.Select(r => r.Value.Name)
            .Should().ContainInOrder("Alice", "Alice", "Bob", "Carol");
    }

    [Fact]
    public void IndexFilter_OrderBy_ThenByDescending_MatchesExample()
    {
        var table = Seed();

        var results = table
            .Query(c => c.Email)
            .AtLeast("user11@example.com")
            .AtMost("user14@example.com")
            .OrderBy(c => c.Name)
            .OrderByDescending(c => c.Age)
            .ToList();

        // Primary Name asc, secondary Age desc:
        // Alice/40 (key 3), Alice/25 (key 2), Bob/22 (key 4), Carol/30 (key 1)
        results.Select(r => r.Key).Should().ContainInOrder(3, 2, 4, 1);
    }

    [Fact]
    public void KeyRangeFilter_OrderBy_Field()
    {
        var table = Seed();

        var results = table
            .Query()
            .AtLeast(1)
            .AtMost(5)
            .OrderByDescending(c => c.Age)
            .ToList();

        results.Select(r => r.Value.Age).Should().ContainInOrder(50, 40, 30, 25, 22);
    }

    [Fact]
    public void OrderByKeyDescending_SortsByPrimaryKey()
    {
        var table = Seed();

        var results = table
            .Query(c => c.City)
            .Equals("NYC")
            .OrderByKeyDescending()
            .ToList();

        // NYC keys are 1, 3, 5 → descending
        results.Select(r => r.Key).Should().ContainInOrder(5, 3, 1);
    }

    [Fact]
    public void OrderBy_Take_AppliesAfterSort()
    {
        var table = Seed();

        var results = table
            .Query()
            .OrderByDescending(c => c.Age)
            .Take(2)
            .ToList();

        results.Select(r => r.Value.Age).Should().ContainInOrder(50, 40);
        results.Should().HaveCount(2);
    }

    [Fact]
    public void OrderBy_IsStable_OnEqualKeys()
    {
        var table = Seed();

        // All five share no single Name, but Alice appears twice (keys 3 then 2 in source order).
        // Source order for index "Email" range is ascending email → key 2 (user12) before key 3 (user13).
        var results = table
            .Query(c => c.Email)
            .AtLeast("user11@example.com")
            .AtMost("user14@example.com")
            .OrderBy(c => c.Name)
            .ToList();

        // Both Alices first; stable sort keeps email order: user12 (key 2) before user13 (key 3).
        var alices = results.Where(r => r.Value.Name == "Alice").Select(r => r.Key).ToList();
        alices.Should().ContainInOrder(2, 3);
    }
}
