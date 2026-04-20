using CatDb.Database;
using CatDb.Tests.Data;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// Tests for ITable navigation: Forward, Backward, FindNext, FindAfter, FindPrev, FindBefore.
/// These use the SortedSet internals and were a crash source during migration.
/// </summary>
public class TableNavigationTests : IDisposable
{
    private readonly IStorageEngine _engine;
    private readonly ITable<int, string> _table;

    public TableNavigationTests()
    {
        _engine = CatDb.Database.CatDb.FromMemory();
        _table = _engine.OpenXTable<int, string>("t");

        // Populate: keys 10, 20, 30, 40, 50
        foreach (var k in new[] { 30, 10, 50, 40, 20 })
            _table[k] = k.ToString();
        _engine.Commit();
    }

    public void Dispose() => _engine.Dispose();

    // ── Forward ───────────────────────────────────────────────────────────────

    [Fact]
    public void Forward_AllRecords_AreInAscendingOrder()
    {
        var keys = _table.Forward().Select(kv => kv.Key).ToList();
        keys.Should().BeInAscendingOrder();
        keys.Should().Equal(10, 20, 30, 40, 50);
    }

    [Fact]
    public void Forward_WithFromBound_StartsAtBoundary()
    {
        var keys = _table.Forward(25, true, 0, false).Select(kv => kv.Key).ToList();
        keys.Should().Equal(30, 40, 50);
    }

    [Fact]
    public void Forward_WithFromAndToBound_ReturnsRange()
    {
        var keys = _table.Forward(20, true, 40, true).Select(kv => kv.Key).ToList();
        keys.Should().Equal(20, 30, 40);
    }

    [Fact]
    public void Forward_WithExactKeyAsFrom_IncludesThatKey()
    {
        var keys = _table.Forward(20, true, 0, false).Select(kv => kv.Key).ToList();
        keys.Should().StartWith([20]);
    }

    // ── Backward ─────────────────────────────────────────────────────────────

    [Fact]
    public void Backward_AllRecords_AreInDescendingOrder()
    {
        var keys = _table.Backward().Select(kv => kv.Key).ToList();
        keys.Should().BeInDescendingOrder();
        keys.Should().Equal(50, 40, 30, 20, 10);
    }

    [Fact]
    public void Backward_WithToBound_StopsAtBoundary()
    {
        var keys = _table.Backward(0, false, 25, true).Select(kv => kv.Key).ToList();
        keys.Should().Equal(50, 40, 30);
    }

    // ── FindNext ──────────────────────────────────────────────────────────────

    [Fact]
    public void FindNext_ExistingKey_ReturnsThatKey()
    {
        var result = _table.FindNext(30);
        result.Should().NotBeNull();
        result!.Value.Key.Should().Be(30);
    }

    [Fact]
    public void FindNext_BetweenKeys_ReturnsNextHigher()
    {
        var result = _table.FindNext(25);
        result.Should().NotBeNull();
        result!.Value.Key.Should().Be(30);
    }

    [Fact]
    public void FindNext_BeyondMax_ReturnsNull()
    {
        _table.FindNext(100).Should().BeNull();
    }

    // ── FindAfter ─────────────────────────────────────────────────────────────

    [Fact]
    public void FindAfter_ExistingKey_ReturnsNextKey()
    {
        var result = _table.FindAfter(30);
        result.Should().NotBeNull();
        result!.Value.Key.Should().Be(40);
    }

    [Fact]
    public void FindAfter_MaxKey_ReturnsNull()
    {
        _table.FindAfter(50).Should().BeNull();
    }

    // ── FindPrev ──────────────────────────────────────────────────────────────

    [Fact]
    public void FindPrev_ExistingKey_ReturnsThatKey()
    {
        var result = _table.FindPrev(30);
        result.Should().NotBeNull();
        result!.Value.Key.Should().Be(30);
    }

    [Fact]
    public void FindPrev_BetweenKeys_ReturnsPreviousLower()
    {
        var result = _table.FindPrev(25);
        result.Should().NotBeNull();
        result!.Value.Key.Should().Be(20);
    }

    [Fact]
    public void FindPrev_BelowMin_ReturnsNull()
    {
        _table.FindPrev(5).Should().BeNull();
    }

    // ── FindBefore ────────────────────────────────────────────────────────────

    [Fact]
    public void FindBefore_ExistingKey_ReturnsPreviousKey()
    {
        var result = _table.FindBefore(30);
        result.Should().NotBeNull();
        result!.Value.Key.Should().Be(20);
    }

    [Fact]
    public void FindBefore_MinKey_ReturnsNull()
    {
        _table.FindBefore(10).Should().BeNull();
    }

    // ── Enumeration ───────────────────────────────────────────────────────────

    [Fact]
    public void Enumeration_DefaultEnumerator_AscendingOrder()
    {
        var keys = _table.Select(kv => kv.Key).ToList();
        keys.Should().BeInAscendingOrder();
    }

    [Fact]
    public void EmptyTable_Forward_YieldsNothing()
    {
        var empty = _engine.OpenXTable<int, string>("empty");
        empty.Forward().Should().BeEmpty();
    }

    [Fact]
    public void EmptyTable_Backward_YieldsNothing()
    {
        var empty = _engine.OpenXTable<int, string>("empty2");
        empty.Backward().Should().BeEmpty();
    }
}
