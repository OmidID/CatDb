// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;
using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// Verifies that index range / equality / prefix scans use exact sentinel bounds (WTree seeks)
/// rather than scanning from an end and skipping — in particular that NEGATIVE primary keys are not
/// lost (the old (field, default) lower bound skipped pk &lt; 0), and that multi-column and
/// mixed-direction composite prefixes work.
/// </summary>
public class IndexSeekTests : IDisposable
{
    private readonly IStorageEngine _engine;

    public class Row
    {
        public string City { get; set; } = "";
        public int Age { get; set; }
        public int Score { get; set; }
    }

    public IndexSeekTests() => _engine = CatDb.Database.CatDb.FromMemory();
    public void Dispose() => _engine.Dispose();

    [Fact]
    public void NonUniqueEquality_IncludesNegativePrimaryKeys()
    {
        var table = _engine.OpenXTable<int, Row>("neg");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        // Mix of negative and positive keys all sharing City = "nyc".
        var keys = new[] { -500, -3, -1, 0, 2, 99 };
        foreach (var k in keys)
            table.Replace(k, new Row { City = "nyc", Age = k });
        table.Replace(7, new Row { City = "la", Age = 7 });
        _engine.Commit();

        table.CountByIndex<int, Row, string>("City", "nyc").Should().Be(keys.Length);
        var got = table.Query(c => c.City).Equal("nyc").Select(r => r.Key).OrderBy(x => x).ToList();
        got.Should().Equal(keys); // every negative key present
    }

    [Fact]
    public void NonUniqueRange_NegativeKeys_BothDirections()
    {
        var table = _engine.OpenXTable<int, Row>("negrange");
        table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);

        for (int k = -50; k <= 50; k++)
            table.Replace(k, new Row { Age = k % 7, City = "x" }); // ages -6..6, dup across +/- keys
        _engine.Commit();

        // Range Age in [-2, 2]; ascending then descending — must include negative-key rows.
        var asc = table.Query(c => c.Age).Between(-2, 2)
            .OrderBy(c => c.Age).Select(r => r.Key).ToList();
        var expectedKeys = Enumerable.Range(-50, 101).Where(k => { var a = k % 7; return a >= -2 && a <= 2; }).ToList();

        asc.Should().BeEquivalentTo(expectedKeys);
        asc.Select(k => k % 7).Should().BeInAscendingOrder();

        var desc = table.Query(c => c.Age).Between(-2, 2)
            .OrderByDescending(c => c.Age).Select(r => r.Key).ToList();
        desc.Should().BeEquivalentTo(expectedKeys);
        desc.Select(k => k % 7).Should().BeInDescendingOrder();
    }

    [Fact]
    public void NonUniqueRange_Descending_SeeksUpperBound_NotTailScan()
    {
        var table = _engine.OpenXTable<int, Row>("seek");
        table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);
        for (int k = 0; k < 5000; k++)
            table.Replace(k, new Row { Age = k }); // distinct ages 0..4999
        _engine.Commit();

        // Descending with a tight upper bound: must come back from 20 down to 10 (not from 4999).
        var got = table.Query(c => c.Age).Between(10, 20)
            .OrderByDescending(c => c.Age).Select(r => r.Value.Age).ToList();

        got.Should().Equal(20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10);
    }

    [Fact]
    public void MultiColumnPrefix_EqualsTwo_OrderByThird()
    {
        var table = _engine.OpenXTable<int, Row>("multi");
        // Composite (City, Age, Score): equality on (City, Age) prefix, order by Score.
        table.CreateIndex("CityAgeScore", new[] { "City", "Age", "Score" }, IndexType.NonUnique);

        var all = new List<KeyValuePair<int, Row>>();
        for (int i = 0; i < 400; i++)
        {
            var r = new Row { City = i % 2 == 0 ? "nyc" : "la", Age = i % 3, Score = (i * 31) % 100 };
            table.Replace(i, r);
            all.Add(new(i, r));
        }
        _engine.Commit();

        // WHERE City='nyc' AND Age=1 ORDER BY Score — prefix length 2.
        var prefix = new Data<Slots<string, int>>(new Slots<string, int>("nyc", 1));
        var got = table.Indexes.FindByIndexPrefix("CityAgeScore", prefix, 2, backward: false)
            .Select(kv => ((Data<int>)kv.Key).Value).ToList();

        var expected = all.Where(kv => kv.Value.City == "nyc" && kv.Value.Age == 1)
            .OrderBy(kv => kv.Value.Score).ThenBy(kv => kv.Key)
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);
    }

    [Fact]
    public void Prefix_MixedDirection_Trailing()
    {
        var table = _engine.OpenXTable<int, Row>("mix");
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);
        table.CreateIndex("CityAgeScore", new[] { "City", "Age", "Score" }, IndexType.NonUnique);

        var all = new List<KeyValuePair<int, Row>>();
        for (int i = 0; i < 600; i++)
        {
            var r = new Row { City = i % 2 == 0 ? "nyc" : "la", Age = i % 4, Score = (i * 17) % 50 };
            table.Replace(i, r);
            all.Add(new(i, r));
        }
        _engine.Commit();

        // WHERE City='nyc' ORDER BY Age ASC, Score DESC — prefix (City) then mixed trailing.
        var got = table.Query(c => c.City).Equal("nyc")
            .OrderBy(c => c.Age)
            .ThenByDescending(c => c.Score)
            .Select(r => r.Key).ToList();

        var expected = all.Where(kv => kv.Value.City == "nyc")
            .OrderBy(kv => kv.Value.Age)
            .ThenByDescending(kv => kv.Value.Score)
            .ThenBy(kv => kv.Key)
            .Select(kv => kv.Key).ToList();

        got.Should().Equal(expected);
    }
}
