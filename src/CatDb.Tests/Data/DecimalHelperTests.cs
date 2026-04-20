using CatDb.General.Extensions;
using FluentAssertions;

namespace CatDb.Tests.Data;

/// <summary>
/// Tests for DecimalHelper — the Expression-tree-based decimal serialization helper.
/// The Constructor was previously using a NonPublic reflection call that broke on .NET 8+;
/// it was replaced with the public decimal(int[]) constructor.
/// </summary>
public class DecimalHelperTests
{
    private readonly DecimalHelper _helper = DecimalHelper.Instance;

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(28)]  // max scale for decimal
    public void GetDigits_KnownValues_ReturnsExpectedScale(int expectedDigits)
    {
        // Build a decimal with a known scale using the decimal(int[]) constructor.
        // bits[3] encodes the scale in bits 16-23: scale << 16.
        // e.g. scale=2 → bits = {1,0,0,0x00020000} → value = 0.01
        var bits = new[] { 1, 0, 0, expectedDigits << 16 };
        var value = new decimal(bits);
        _helper.GetDigits(ref value).Should().Be(expectedDigits);
    }

    [Theory]
    [InlineData(0, 0, 0, 0)]               // 0
    [InlineData(1, 0, 0, 0)]               // 1
    [InlineData(100, 0, 0, 0)]             // 100
    [InlineData(1, 0, 0, unchecked((int)0x80010000))]  // -0.1
    public void Constructor_MatchesExpectedDecimal(int lo, int mid, int hi, int flags)
    {
        var bits = new int[] { lo, mid, hi, flags };
        var expected = new decimal(bits);
        var actual = _helper.Constructor(lo, mid, hi, flags);
        actual.Should().Be(expected);
    }

    [Fact]
    public void Constructor_RoundTrip_AllBitsPreserved()
    {
        var testValues = new[]
        {
            0m, 1m, -1m, decimal.MaxValue, decimal.MinValue,
            0.1m, 0.01m, 1234567890.123456789m, -9999999999.9999999m
        };

        foreach (var original in testValues)
        {
            var bits = decimal.GetBits(original);
            var reconstructed = _helper.Constructor(bits[0], bits[1], bits[2], bits[3]);
            reconstructed.Should().Be(original, $"round-trip failed for {original}");
        }
    }
}
