// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// Cross-index "drive-from-sort-index": filter on one index/key, ORDER BY a different indexed
/// field. Iteration is driven from the sort field's index (already sorted) with the original
/// filter re-applied as a residual; multi-key sorts run-sort within equal-leading-key groups.
/// Each test checks the engine result against an in-memory LINQ reference.
/// </summary>
public class CrossIndexSortTests : IDisposable
{
    private readonly IStorageEngine _engine;

    public class Customer
    {
        public string Email { get; set; } = "";
        public string City { get; set; } = "";
        public int Age { get; set; }
        public string Name { get; set; } = "";
    }

    public CrossIndexSortTests() => _engine = CatDb.Database.CatDb.FromMemory();
    public void Dispose() => _engine.Dispose();

    // Lowercase, distinct-prefix names → ordinal order == default order (no culture flakiness).
    private static readonly string[] Names = { "ann", "bob", "cy", "dan", "eve" };

    private (ITable<int, Customer> table, List<KeyValuePair<int, Customer>> all) Seed(int n = 500)
    {
        var table = _engine.OpenXTable<int, Customer>("customers");
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);
        table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);
        table.CreateIndex("Name", c => c.Name, IndexType.NonUnique);

        var all = new List<KeyValuePair<int, Customer>>(n);
        for (int i = 0; i < n; i++)
        {
            var c = new Customer
            {
                Email = $"u{i:D3}@x.com",
                City = i % 2 == 0 ? "nyc" : "la",
                Age = (i * 7) % 50,
                Name = Names[i % Names.Length],
            };
            table.Replace(i, c);
            all.Add(new KeyValuePair<int, Customer>(i, c));
        }
        _engine.Commit();
        return (table, all);
    }

    // ── Single-key cross-index drive ──────────────────────────────────────────

    [Fact]
    public void FilterByCity_OrderByAge_DrivesFromAgeIndex()
    {
        var (table, all) = Seed();

        var got = table.Query(c => c.City).Equal("nyc")
            .OrderBy(c => c.Age)
            .Select(r => r.Key).ToList();

        var expected = all.Where(kv => kv.Value.City == "nyc")
            .OrderBy(kv => kv.Value.Age)
            .Select(kv => kv.Key).ToList();

        // Same set, ages ascending; key order within equal age is not asserted (only Age is the key).
        got.Should().BeEquivalentTo(expected);
        got.Select(k => all[k].Value.Age).Should().BeInAscendingOrder();
        got.Should().OnlyContain(k => all[k].Value.City == "nyc");
    }

    [Fact]
    public void FilterByCity_OrderByAgeDescending_DrivesBackward()
    {
        var (table, all) = Seed();

        var ages = table.Query(c => c.City).Equal("la")
            .OrderByDescending(c => c.Age)
            .Select(r => r.Value.Age).ToList();

        ages.Should().BeInDescendingOrder();
        ages.Should().HaveCount(all.Count(kv => kv.Value.City == "la"));
    }

    [Fact]
    public void FilterByKeyRange_OrderByIndexedField_DrivesWithKeyResidual()
    {
        var (table, all) = Seed();

        var got = table.Query().KeyBetween(100, 399)
            .OrderBy(c => c.Age)
            .Select(r => r.Key).ToList();

        got.Should().HaveCount(300);
        got.Should().OnlyContain(k => k >= 100 && k <= 399);
        got.Select(k => all[k].Value.Age).Should().BeInAscendingOrder();
    }

    [Fact]
    public void ExclusiveFilter_OrderByIndexedField_ResidualIsExact()
    {
        var (table, all) = Seed();

        var got = table.Query(c => c.Age).GreaterThan(10) // exclusive
            .OrderBy(c => c.Name)                          // drive from Name index
            .Select(r => r.Key).ToList();

        got.Should().OnlyContain(k => all[k].Value.Age > 10);
        got.Should().HaveCount(all.Count(kv => kv.Value.Age > 10));
        got.Select(k => all[k].Value.Name).Should().BeInAscendingOrder(StringComparer.Ordinal);
    }

    // ── Multi-key drive: leading indexed, run-sort the rest ───────────────────

    [Fact]
    public void Headline_FilterEmailRange_OrderByName_ThenByAgeDesc()
    {
        var (table, all) = Seed();

        var got = table.Query(c => c.Email)
            .AtLeast("u100@x.com").AtMost("u399@x.com")
            .OrderBy(c => c.Name)            // leading key: Name index drives
            .ThenByDescending(c => c.Age)   // secondary: run-sorted within equal Name
            .Select(r => r.Key).ToList();

        var expected = all
            .Where(kv => string.CompareOrdinal(kv.Value.Email, "u100@x.com") >= 0
                      && string.CompareOrdinal(kv.Value.Email, "u399@x.com") <= 0)
            .OrderBy(kv => kv.Value.Name, StringComparer.Ordinal)
            .ThenByDescending(kv => kv.Value.Age)
            .ThenBy(kv => kv.Key)            // stable tie-break, matches run-sort stability
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);
    }

    [Fact]
    public void MultiKeyDrive_Take_StreamsCorrectPrefix()
    {
        var (table, all) = Seed();

        var got = table.Query(c => c.City).Equal("nyc")
            .OrderBy(c => c.Name)
            .ThenByDescending(c => c.Age)
            .Take(7)
            .Select(r => r.Key).ToList();

        var expected = all.Where(kv => kv.Value.City == "nyc")
            .OrderBy(kv => kv.Value.Name, StringComparer.Ordinal)
            .ThenByDescending(kv => kv.Value.Age)
            .ThenBy(kv => kv.Key)
            .Take(7)
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);
    }

    // ── Drive result must equal the buffered-sort result (consistency) ────────

    [Fact]
    public void SameField_Leading_ThenBySecondary_RunSorts()
    {
        var (table, all) = Seed();

        // Leading key is the index's OWN field (Age) — driven from the source scan — then ThenBy Name.
        var got = table.Query(c => c.Age).Between(10, 30)
            .OrderBy(c => c.Age)
            .ThenByDescending(c => c.Name)
            .Select(r => r.Key).ToList();

        var expected = all.Where(kv => kv.Value.Age >= 10 && kv.Value.Age <= 30)
            .OrderBy(kv => kv.Value.Age)
            .ThenByDescending(kv => kv.Value.Name, StringComparer.Ordinal)
            .ThenBy(kv => kv.Key)
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);
    }

    [Fact]
    public void Drive_And_Buffer_AgreeOnSameQuery()
    {
        var (table, all) = Seed(300);

        // Drive path (Age indexed):
        var driven = table.Query(c => c.City).Equal("nyc")
            .OrderBy(c => c.Age).ThenBy(c => c.Email)
            .Select(r => r.Key).ToList();

        // Reference buffered sort with the same comparers:
        var reference = all.Where(kv => kv.Value.City == "nyc")
            .OrderBy(kv => kv.Value.Age)
            .ThenBy(kv => kv.Value.Email, StringComparer.Ordinal)
            .Select(kv => kv.Key).ToList();

        driven.Should().Equal(reference);
    }

    // ── Covering composite index: ORDER BY (a, b) streams from the (a,b) index ─

    /// <summary>Table whose ONLY index is the composite (City, Age) — so the covering-composite
    /// path is the only streaming option (no single-field City/Age index to fall back to).</summary>
    private (ITable<int, Customer> table, List<KeyValuePair<int, Customer>> all) SeedComposite(int n = 400)
    {
        var table = _engine.OpenXTable<int, Customer>("composite");
        table.CreateIndex("CityAge", new[] { "City", "Age" }, IndexType.NonUnique);

        var all = new List<KeyValuePair<int, Customer>>(n);
        for (int i = 0; i < n; i++)
        {
            // Few distinct (City, Age) pairs → many duplicates across different keys.
            // Email is distinct but NOT in key order (bijection mod 100000) so Top-K is non-trivial.
            var c = new Customer
            {
                Email = $"e{(i * 7919) % 100000:D5}",
                City = i % 3 == 0 ? "nyc" : "la",
                Age = i % 4,
                Name = Names[i % Names.Length],
            };
            table.Replace(i, c);
            all.Add(new KeyValuePair<int, Customer>(i, c));
        }
        _engine.Commit();
        return (table, all);
    }

    [Fact]
    public void CompositeIndex_AscendingCovering_MatchesReference()
    {
        var (table, all) = SeedComposite();

        var got = table.Query().KeyBetween(50, 349)
            .OrderBy(c => c.City)
            .ThenBy(c => c.Age)
            .Select(r => r.Key).ToList();

        var expected = all.Where(kv => kv.Key >= 50 && kv.Key <= 349)
            .OrderBy(kv => kv.Value.City, StringComparer.Ordinal)
            .ThenBy(kv => kv.Value.Age)
            .ThenBy(kv => kv.Key) // composite forward orders ties by pk asc
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);
    }

    [Fact]
    public void CompositeIndex_MixedDirection_AscThenDesc()
    {
        var (table, all) = SeedComposite();

        var got = table.Query().KeyBetween(50, 349)
            .OrderBy(c => c.City)             // ascending
            .ThenByDescending(c => c.Age)     // descending — mixed
            .Select(r => r.Key).ToList();

        var expected = all.Where(kv => kv.Key >= 50 && kv.Key <= 349)
            .OrderBy(kv => kv.Value.City, StringComparer.Ordinal)
            .ThenByDescending(kv => kv.Value.Age)
            .ThenBy(kv => kv.Key) // composite forward groups → ties stay pk-ascending
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);
    }

    [Fact]
    public void CompositeIndex_MixedDirection_DescThenAsc()
    {
        var (table, all) = SeedComposite();

        var got = table.Query().KeyBetween(50, 349)
            .OrderByDescending(c => c.City)   // descending lead → composite scanned backward
            .ThenBy(c => c.Age)               // ascending — mixed
            .Select(r => r.Key).ToList();

        var expected = all.Where(kv => kv.Key >= 50 && kv.Key <= 349)
            .OrderByDescending(kv => kv.Value.City, StringComparer.Ordinal)
            .ThenBy(kv => kv.Value.Age)
            .ThenByDescending(kv => kv.Key) // backward groups → ties stay pk-descending
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);
    }

    // ── Top-K bounded heap (buffered + run-sort) ──────────────────────────────

    [Fact]
    public void BufferedTopK_Take_MatchesFullSortThenTake()
    {
        var (table, all) = SeedComposite();

        // Email is NOT indexed in the composite-only table → buffered path. Take triggers Top-K.
        const int k = 17;
        var got = table.Query().KeyBetween(1, 400)
            .OrderBy(c => c.Email)
            .Take(k)
            .Select(r => r.Key).ToList();

        var expected = all.Where(kv => kv.Key >= 1 && kv.Key <= 400)
            .OrderBy(kv => kv.Value.Email, StringComparer.Ordinal)
            .ThenBy(kv => kv.Key)
            .Take(k)
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);
    }

    [Fact]
    public void MixedComposite_Take_TopKWithinGroups()
    {
        var (table, all) = SeedComposite();

        const int k = 23;
        var got = table.Query().KeyBetween(1, 400)
            .OrderBy(c => c.City)
            .ThenByDescending(c => c.Age)
            .Take(k)
            .Select(r => r.Key).ToList();

        var expected = all.Where(kv => kv.Key >= 1 && kv.Key <= 400)
            .OrderBy(kv => kv.Value.City, StringComparer.Ordinal)
            .ThenByDescending(kv => kv.Value.Age)
            .ThenBy(kv => kv.Key)
            .Take(k)
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);
    }

    [Fact]
    public void TopK_Skip_And_Take_Page()
    {
        var (table, all) = SeedComposite();

        var got = table.Query().KeyBetween(1, 400)
            .OrderBy(c => c.Email)
            .Skip(10).Take(15)
            .Select(r => r.Key).ToList();

        var expected = all.Where(kv => kv.Key >= 1 && kv.Key <= 400)
            .OrderBy(kv => kv.Value.Email, StringComparer.Ordinal)
            .ThenBy(kv => kv.Key)
            .Skip(10).Take(15)
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);
    }

    // ── Composite PREFIX scan: WHERE a=v ORDER BY b → single (a,b) range scan ──

    /// <summary>Single-field <c>City</c> index (for the equality filter) + composite (City, Age)
    /// (for the ordered prefix scan), but deliberately NO single <c>Age</c> index — so a buffered
    /// fallback is distinguishable from the prefix scan by tie-break direction.</summary>
    private (ITable<int, Customer> table, List<KeyValuePair<int, Customer>> all) SeedPrefix(int n = 300)
    {
        var table = _engine.OpenXTable<int, Customer>("prefix");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);
        table.CreateIndex("CityAge", new[] { "City", "Age" }, IndexType.NonUnique);

        var all = new List<KeyValuePair<int, Customer>>(n);
        for (int i = 0; i < n; i++)
        {
            var c = new Customer { City = i % 2 == 0 ? "nyc" : "la", Age = i % 5, Name = Names[i % Names.Length] };
            table.Replace(i, c);
            all.Add(new KeyValuePair<int, Customer>(i, c));
        }
        _engine.Commit();
        return (table, all);
    }

    [Fact]
    public void PrefixScan_Equals_OrderByTrailing_Ascending()
    {
        var (table, all) = SeedPrefix();

        var got = table.Query(c => c.City).Equal("nyc")
            .OrderBy(c => c.Age)
            .Select(r => r.Key).ToList();

        var expected = all.Where(kv => kv.Value.City == "nyc")
            .OrderBy(kv => kv.Value.Age)
            .ThenBy(kv => kv.Key) // composite forward → ties pk-ascending
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);
    }

    [Fact]
    public void PrefixScan_Equals_OrderByTrailing_Descending_ProvesPrefixPath()
    {
        var (table, all) = SeedPrefix();

        var got = table.Query(c => c.City).Equal("nyc")
            .OrderByDescending(c => c.Age)
            .Select(r => r.Key).ToList();

        // A backward prefix scan orders equal-Age ties by pk DESCENDING — a buffered fallback would
        // keep source (pk-ascending) order. So matching the desc tie-break proves the prefix path.
        var expected = all.Where(kv => kv.Value.City == "nyc")
            .OrderByDescending(kv => kv.Value.Age)
            .ThenByDescending(kv => kv.Key)
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);
    }

    [Fact]
    public void PrefixScan_Take_StreamsEarlyStop()
    {
        var (table, all) = SeedPrefix();

        var got = table.Query(c => c.City).Equal("nyc")
            .OrderBy(c => c.Age).Take(9)
            .Select(r => r.Key).ToList();

        var expected = all.Where(kv => kv.Value.City == "nyc")
            .OrderBy(kv => kv.Value.Age).ThenBy(kv => kv.Key)
            .Take(9)
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);
    }

    [Fact]
    public void FindByIndexPrefix_Manager_ForwardAndBackward()
    {
        var (table, all) = SeedPrefix();

        var fwd = table.Indexes
            .FindByIndexPrefix("CityAge", new CatDb.Data.Data<string>("la"), 1, backward: false)
            .Select(kv => ((CatDb.Data.Data<int>)kv.Key).Value).ToList();
        var laByAge = all.Where(kv => kv.Value.City == "la")
            .OrderBy(kv => kv.Value.Age).ThenBy(kv => kv.Key).Select(kv => kv.Key).ToList();
        fwd.Should().Equal(laByAge);

        var bwd = table.Indexes
            .FindByIndexPrefix("CityAge", new CatDb.Data.Data<string>("la"), 1, backward: true)
            .Select(kv => ((CatDb.Data.Data<int>)kv.Key).Value).ToList();
        var laByAgeDesc = all.Where(kv => kv.Value.City == "la")
            .OrderByDescending(kv => kv.Value.Age).ThenByDescending(kv => kv.Key).Select(kv => kv.Key).ToList();
        bwd.Should().Equal(laByAgeDesc);
    }

    [Fact]
    public void CompositeIndex_DescendingCovering_DrivesBackward()
    {
        var (table, all) = SeedComposite();

        var got = table.Query().KeyBetween(50, 349)
            .OrderByDescending(c => c.City)
            .ThenByDescending(c => c.Age)
            .Select(r => r.Key).ToList();

        // Path proof: a backward composite scan yields ties (equal City+Age) in pk-DESCENDING order —
        // a buffered/stable fallback would keep the source (key-ascending) order instead.
        var expected = all.Where(kv => kv.Key >= 50 && kv.Key <= 349)
            .OrderByDescending(kv => kv.Value.City, StringComparer.Ordinal)
            .ThenByDescending(kv => kv.Value.Age)
            .ThenByDescending(kv => kv.Key)
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);

        // And the (City, Age) projection is monotonically non-increasing.
        var pairs = got.Select(k => (all[k].Value.City, all[k].Value.Age)).ToList();
        for (int i = 1; i < pairs.Count; i++)
        {
            var cmp = string.CompareOrdinal(pairs[i - 1].City, pairs[i].City);
            (cmp > 0 || (cmp == 0 && pairs[i - 1].Age >= pairs[i].Age)).Should().BeTrue();
        }
    }
}
