// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;
using FluentAssertions;

namespace CatDb.Tests.Data;

/// <summary>
/// Tests for DataPersist — the Expression-tree-based binary serializer/deserializer for IData.
/// Round-trip tests for all supported primitive types and a complex class.
/// </summary>
public class DataPersistTests
{
    private static T RoundTrip<T>(T value)
    {
        var persist = new DataPersist(typeof(T));
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        persist.Write(writer, new CatDb.Data.Data<T>(value));
        ms.Position = 0;
        using var reader = new BinaryReader(ms);
        var result = persist.Read(reader);
        return ((CatDb.Data.Data<T>)result).Value;
    }

    [Fact]
    public void Bool_RoundTrips() => RoundTrip(true).Should().Be(true);

    [Fact]
    public void Byte_RoundTrips() => RoundTrip((byte)255).Should().Be(255);

    [Fact]
    public void Int16_RoundTrips() => RoundTrip((short)-32768).Should().Be(-32768);

    [Fact]
    public void Int32_RoundTrips() => RoundTrip(int.MinValue).Should().Be(int.MinValue);

    [Fact]
    public void Int64_RoundTrips() => RoundTrip(long.MaxValue).Should().Be(long.MaxValue);

    [Fact]
    public void UInt64_RoundTrips() => RoundTrip(ulong.MaxValue).Should().Be(ulong.MaxValue);

    [Fact]
    public void Float_RoundTrips() => RoundTrip(3.14f).Should().BeApproximately(3.14f, 0.0001f);

    [Fact]
    public void Double_RoundTrips() => RoundTrip(double.Epsilon).Should().Be(double.Epsilon);

    [Fact]
    public void Decimal_RoundTrips()
    {
        var testValues = new[] { 0m, 1m, -1m, decimal.MaxValue, decimal.MinValue, 1234567890.123456789m };
        foreach (var v in testValues)
            RoundTrip(v).Should().Be(v, $"decimal round-trip failed for {v}");
    }

    [Fact]
    public void DateTime_RoundTrips()
    {
        var d = new DateTime(2025, 12, 31, 23, 59, 59, DateTimeKind.Utc);
        RoundTrip(d).Should().Be(d);
    }

    [Fact]
    public void TimeSpan_RoundTrips()
    {
        var ts = TimeSpan.FromHours(48.5);
        RoundTrip(ts).Should().Be(ts);
    }

    [Fact]
    public void String_RoundTrips()
    {
        RoundTrip("Hello, CatDb! 🐱").Should().Be("Hello, CatDb! 🐱");
    }

    [Fact]
    public void String_EmptyString_RoundTrips()
    {
        RoundTrip("").Should().Be("");
    }

    [Fact]
    public void ByteArray_RoundTrips()
    {
        var data = new byte[] { 0, 1, 127, 128, 255 };
        RoundTrip(data).Should().BeEquivalentTo(data);
    }

    [Fact]
    public void ComplexClass_RoundTrips()
    {
        var tick = new Tick("AAPL", new DateTime(2025, 1, 15, 9, 30, 0), 182.5, 182.51, 100, 200, "Bloomberg");
        var persist = new DataPersist(typeof(Tick));
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        persist.Write(writer, new CatDb.Data.Data<Tick>(tick));
        ms.Position = 0;
        using var reader = new BinaryReader(ms);
        var result = ((CatDb.Data.Data<Tick>)persist.Read(reader)).Value;

        result.Symbol.Should().Be(tick.Symbol);
        result.Timestamp.Should().Be(tick.Timestamp);
        result.Bid.Should().Be(tick.Bid);
        result.Ask.Should().Be(tick.Ask);
        result.BidSize.Should().Be(tick.BidSize);
        result.AskSize.Should().Be(tick.AskSize);
        result.Provider.Should().Be(tick.Provider);
    }

    [Fact]
    public void Struct_RoundTrips()
    {
        var key = new TestKey { Id = 42, Name = "test-key" };
        var persist = new DataPersist(typeof(TestKey));
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        persist.Write(writer, new CatDb.Data.Data<TestKey>(key));
        ms.Position = 0;
        using var reader = new BinaryReader(ms);
        var result = ((CatDb.Data.Data<TestKey>)persist.Read(reader)).Value;
        result.Id.Should().Be(key.Id);
        result.Name.Should().Be(key.Name);
    }

    [Fact]
    public void MultipleValues_SequentialWrite_SequentialRead_RoundTrips()
    {
        var persist = new DataPersist(typeof(int));
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        for (var i = 0; i < 100; i++)
            persist.Write(writer, new CatDb.Data.Data<int>(i));

        ms.Position = 0;
        using var reader = new BinaryReader(ms);
        for (var i = 0; i < 100; i++)
            ((CatDb.Data.Data<int>)persist.Read(reader)).Value.Should().Be(i);
    }
}
