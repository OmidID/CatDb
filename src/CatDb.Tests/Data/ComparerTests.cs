using CatDb.Data;
using FluentAssertions;

namespace CatDb.Tests.Data;

/// <summary>
/// Tests for Comparer&lt;T&gt; — the Expression-tree-compiled comparers used as B-tree key comparers.
/// These were a source of crashes when types with string/decimal/byte[] fields were used.
/// </summary>
public class ComparerTests
{
    [Fact]
    public void Int32_Compare_ReturnsCorrectOrdering()
    {
        var cmp = new CatDb.Data.Comparer<int>();
        cmp.Compare(1, 2).Should().BeNegative();
        cmp.Compare(2, 1).Should().BePositive();
        cmp.Compare(5, 5).Should().Be(0);
    }

    [Fact]
    public void Int64_Compare_ReturnsCorrectOrdering()
    {
        var cmp = new CatDb.Data.Comparer<long>();
        cmp.Compare(long.MinValue, long.MaxValue).Should().BeNegative();
        cmp.Compare(long.MaxValue, long.MinValue).Should().BePositive();
        cmp.Compare(0L, 0L).Should().Be(0);
    }

    [Fact]
    public void String_Compare_ReturnsCorrectOrdering()
    {
        var cmp = new CatDb.Data.Comparer<string>();
        cmp.Compare("apple", "banana").Should().BeNegative();
        cmp.Compare("banana", "apple").Should().BePositive();
        cmp.Compare("same", "same").Should().Be(0);
    }

    [Fact]
    public void String_Compare_EmptyVsNull_DoesNotThrow()
    {
        var cmp = new CatDb.Data.Comparer<string>();
        var act = () => cmp.Compare(null!, "hello");
        act.Should().NotThrow();
    }

    [Fact]
    public void Double_Compare_ReturnsCorrectOrdering()
    {
        var cmp = new CatDb.Data.Comparer<double>();
        cmp.Compare(1.1, 2.2).Should().BeNegative();
        cmp.Compare(double.MinValue, double.MaxValue).Should().BeNegative();
        cmp.Compare(3.14, 3.14).Should().Be(0);
    }

    [Fact]
    public void Decimal_Compare_ReturnsCorrectOrdering()
    {
        var cmp = new CatDb.Data.Comparer<decimal>();
        cmp.Compare(1.0m, 2.0m).Should().BeNegative();
        cmp.Compare(100.99m, 100.99m).Should().Be(0);
        cmp.Compare(decimal.MaxValue, decimal.MinValue).Should().BePositive();
    }

    [Fact]
    public void DateTime_Compare_ReturnsCorrectOrdering()
    {
        var cmp = new CatDb.Data.Comparer<DateTime>();
        var d1 = new DateTime(2020, 1, 1);
        var d2 = new DateTime(2025, 1, 1);
        cmp.Compare(d1, d2).Should().BeNegative();
        cmp.Compare(d2, d1).Should().BePositive();
        cmp.Compare(d1, d1).Should().Be(0);
    }

    [Fact]
    public void Guid_Compare_ReturnsCorrectOrdering()
    {
        var cmp = new CatDb.Data.Comparer<Guid>();
        var g1 = new Guid("00000000-0000-0000-0000-000000000001");
        var g2 = new Guid("00000000-0000-0000-0000-000000000002");
        cmp.Compare(g1, g2).Should().BeNegative();
        cmp.Compare(g2, g1).Should().BePositive();
        cmp.Compare(g1, g1).Should().Be(0);
    }

    [Fact]
    public void Struct_WithMultipleFields_ComparesLexicographically()
    {
        var cmp = new CatDb.Data.Comparer<TestKey>();
        var a = new TestKey { Id = 1, Name = "alpha" };
        var b = new TestKey { Id = 1, Name = "beta" };
        var c = new TestKey { Id = 2, Name = "alpha" };

        cmp.Compare(a, b).Should().BeNegative();  // same Id, Name differs
        cmp.Compare(c, a).Should().BePositive();  // Id differs
        cmp.Compare(a, a).Should().Be(0);
    }

    [Fact]
    public void ByteArray_Compare_ReturnsCorrectOrdering()
    {
        var cmp = new CatDb.Data.Comparer<byte[]>();
        cmp.Compare(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 4 }).Should().BeNegative();
        cmp.Compare(new byte[] { 5 }, new byte[] { 1, 2, 3 }).Should().BePositive();
        cmp.Compare(new byte[] { 1, 2 }, new byte[] { 1, 2 }).Should().Be(0);
    }
}
