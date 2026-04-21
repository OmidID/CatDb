using CatDb.Database;
using CatDb.Extensions;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// Unit tests for KeyQuery&lt;TKey&gt; + TableQueryExtensions (Query, QueryBackward,
/// Count, Page, PageAfter).  Uses an in-memory engine so no disk I/O.
/// </summary>
public class KeyQueryTests : IDisposable
{
    private readonly IStorageEngine _engine;
    private readonly ITable<int, string> _ints;
    private readonly ITable<string, string> _strings;

    public KeyQueryTests()
    {
        _engine = CatDb.Database.CatDb.FromMemory();

        _ints = _engine.OpenXTable<int, string>("ints");
        foreach (var k in new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 })
            _ints[k] = k.ToString();

        _strings = _engine.OpenXTable<string, string>("strings");
        foreach (var s in new[] { "apple", "apricot", "avocado", "banana", "cherry", "date" })
            _strings[s] = s.ToUpper();

        _engine.Commit();
    }

    public void Dispose() => _engine.Dispose();

    // ── KeyQuery<int> factories ───────────────────────────────────────────────

    [Fact]
    public void All_ReturnsEveryRecord()
    {
        var keys = _ints.Query(KeyQuery<int>.All()).Select(kv => kv.Key).ToList();
        keys.Should().BeEquivalentTo(Enumerable.Range(1, 10));
    }

    [Fact]
    public void AtLeast_ReturnsFromBoundInclusive()
    {
        var keys = _ints.Query(KeyQuery<int>.AtLeast(5)).Select(kv => kv.Key).ToList();
        keys.Should().Equal(5, 6, 7, 8, 9, 10);
    }

    [Fact]
    public void GreaterThan_ExcludesLowerBoundKey()
    {
        var keys = _ints.Query(KeyQuery<int>.GreaterThan(5)).Select(kv => kv.Key).ToList();
        keys.Should().Equal(6, 7, 8, 9, 10);
        keys.Should().NotContain(5);
    }

    [Fact]
    public void AtMost_ReturnsUpToBoundInclusive()
    {
        var keys = _ints.Query(KeyQuery<int>.AtMost(4)).Select(kv => kv.Key).ToList();
        keys.Should().Equal(1, 2, 3, 4);
    }

    [Fact]
    public void LessThan_ExcludesUpperBoundKey()
    {
        var keys = _ints.Query(KeyQuery<int>.LessThan(4)).Select(kv => kv.Key).ToList();
        keys.Should().Equal(1, 2, 3);
        keys.Should().NotContain(4);
    }

    [Fact]
    public void Between_InclusiveBothEnds_ReturnsRange()
    {
        var keys = _ints.Query(KeyQuery<int>.Between(3, 7)).Select(kv => kv.Key).ToList();
        keys.Should().Equal(3, 4, 5, 6, 7);
    }

    [Fact]
    public void Between_ExclusiveLower_ExcludesFrom()
    {
        var keys = _ints.Query(KeyQuery<int>.Between(3, 7, fromInclusive: false)).Select(kv => kv.Key).ToList();
        keys.Should().Equal(4, 5, 6, 7);
        keys.Should().NotContain(3);
    }

    [Fact]
    public void Between_ExclusiveUpper_ExcludesTo()
    {
        var keys = _ints.Query(KeyQuery<int>.Between(3, 7, toInclusive: false)).Select(kv => kv.Key).ToList();
        keys.Should().Equal(3, 4, 5, 6);
        keys.Should().NotContain(7);
    }

    [Fact]
    public void Between_BothExclusive_ExcludesBothEnds()
    {
        var keys = _ints.Query(KeyQuery<int>.Between(3, 7, fromInclusive: false, toInclusive: false)).Select(kv => kv.Key).ToList();
        keys.Should().Equal(4, 5, 6);
    }

    // ── KeyQuery.StartsWith ───────────────────────────────────────────────────

    [Fact]
    public void StartsWith_ReturnsOnlyMatchingPrefix()
    {
        var keys = _strings.Query(KeyQuery.StartsWith("a")).Select(kv => kv.Key).ToList();
        keys.Should().Equal("apple", "apricot", "avocado");
    }

    [Fact]
    public void StartsWith_NarrowPrefix_ReturnsSubset()
    {
        var keys = _strings.Query(KeyQuery.StartsWith("ap")).Select(kv => kv.Key).ToList();
        keys.Should().Equal("apple", "apricot");
    }

    [Fact]
    public void StartsWith_ExactKey_ReturnsThatKey()
    {
        var keys = _strings.Query(KeyQuery.StartsWith("banana")).Select(kv => kv.Key).ToList();
        keys.Should().Equal("banana");
    }

    [Fact]
    public void StartsWith_NoMatch_ReturnsEmpty()
    {
        var keys = _strings.Query(KeyQuery.StartsWith("zzz")).Select(kv => kv.Key).ToList();
        keys.Should().BeEmpty();
    }

    // ── QueryBackward ─────────────────────────────────────────────────────────

    [Fact]
    public void QueryBackward_Between_ReturnsDescending()
    {
        var keys = _ints.QueryBackward(KeyQuery<int>.Between(3, 7)).Select(kv => kv.Key).ToList();
        keys.Should().Equal(7, 6, 5, 4, 3);
    }

    [Fact]
    public void QueryBackward_StartsWith_ReturnsDescending()
    {
        var keys = _strings.QueryBackward(KeyQuery.StartsWith("a")).Select(kv => kv.Key).ToList();
        keys.Should().Equal("avocado", "apricot", "apple");
    }

    [Fact]
    public void QueryBackward_GreaterThan_DescendingExcludesBound()
    {
        var keys = _ints.QueryBackward(KeyQuery<int>.GreaterThan(5)).Select(kv => kv.Key).ToList();
        keys.Should().BeInDescendingOrder();
        keys.Should().Equal(10, 9, 8, 7, 6);
    }

    // ── Count ─────────────────────────────────────────────────────────────────

    [Fact]
    public void Count_All_ReturnsTotalRecords()
    {
        _ints.Count(KeyQuery<int>.All()).Should().Be(10);
    }

    [Fact]
    public void Count_Range_ReturnsMatchingCount()
    {
        _ints.Count(KeyQuery<int>.Between(3, 7)).Should().Be(5);
    }

    [Fact]
    public void Count_StartsWith_ReturnsMatchingCount()
    {
        _strings.Count(KeyQuery.StartsWith("a")).Should().Be(3);
    }

    // ── Page (offset) ─────────────────────────────────────────────────────────

    [Fact]
    public void Page_FirstPage_ReturnsFirstN()
    {
        var keys = _ints.Page(KeyQuery<int>.All(), skip: 0, take: 3).Select(kv => kv.Key).ToList();
        keys.Should().Equal(1, 2, 3);
    }

    [Fact]
    public void Page_SecondPage_ReturnsNextN()
    {
        var keys = _ints.Page(KeyQuery<int>.All(), skip: 3, take: 3).Select(kv => kv.Key).ToList();
        keys.Should().Equal(4, 5, 6);
    }

    [Fact]
    public void Page_LastPage_ReturnsRemainder()
    {
        var keys = _ints.Page(KeyQuery<int>.All(), skip: 9, take: 10).Select(kv => kv.Key).ToList();
        keys.Should().Equal(10);
    }

    [Fact]
    public void Page_NegativeSkip_Throws()
    {
        var act = () => _ints.Page(KeyQuery<int>.All(), skip: -1, take: 3).ToList();
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    // ── PageAfter (keyset/cursor paging) ─────────────────────────────────────

    [Fact]
    public void PageAfter_NoCursor_ReturnsFirstPage()
    {
        var keys = _ints.PageAfter(KeyQuery<int>.All(), take: 3).Select(kv => kv.Key).ToList();
        keys.Should().Equal(1, 2, 3);
    }

    [Fact]
    public void PageAfter_WithCursor_ReturnsNextPage()
    {
        var first = _ints.PageAfter(KeyQuery<int>.All(), take: 3).ToList();
        var second = _ints.PageAfter(KeyQuery<int>.All(), afterKey: first.Last().Key, take: 3).Select(kv => kv.Key).ToList();
        second.Should().Equal(4, 5, 6);
    }

    [Fact]
    public void PageAfter_CursorAtEnd_ReturnsEmpty()
    {
        var keys = _ints.PageAfter(KeyQuery<int>.All(), afterKey: 10, take: 10).ToList();
        keys.Should().BeEmpty();
    }

    [Fact]
    public void PageAfter_CanPageThroughAll()
    {
        var all = new List<int>();
        var isFirst = true;
        int cursor = 0;
        const int pageSize = 3;

        while (true)
        {
            var page = isFirst
                ? _ints.PageAfter(KeyQuery<int>.All(), take: pageSize).ToList()
                : _ints.PageAfter(KeyQuery<int>.All(), afterKey: cursor, take: pageSize).ToList();
            if (page.Count == 0) break;
            all.AddRange(page.Select(kv => kv.Key));
            cursor  = page.Last().Key;
            isFirst = false;
        }

        all.Should().Equal(Enumerable.Range(1, 10));
    }

    [Fact]
    public void PageAfter_WithRange_RespectsUpperBound()
    {
        var query = KeyQuery<int>.Between(3, 8);
        var all = new List<int>();
        var isFirst = true;
        int cursor = 0;

        while (true)
        {
            var page = isFirst
                ? _ints.PageAfter(query, take: 2).ToList()
                : _ints.PageAfter(query, afterKey: cursor, take: 2).ToList();
            if (page.Count == 0) break;
            all.AddRange(page.Select(kv => kv.Key));
            cursor  = page.Last().Key;
            isFirst = false;
        }

        all.Should().Equal(3, 4, 5, 6, 7, 8);
    }

    // ── WithFilter (reserved future hook) ────────────────────────────────────

    [Fact]
    public void WithFilter_AppliesPostScanPredicate()
    {
        // Only even keys in range 1-10
        var keys = _ints
            .Query(KeyQuery<int>.All().WithFilter(k => k % 2 == 0))
            .Select(kv => kv.Key).ToList();
        keys.Should().Equal(2, 4, 6, 8, 10);
    }
}
