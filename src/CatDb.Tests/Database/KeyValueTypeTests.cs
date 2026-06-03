// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Tests.Data;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// Tests for all supported key/value type combinations.
/// Verifies the serialization pipeline handles all primitive and compound types without crashing.
/// </summary>
public class KeyValueTypeTests : IDisposable
{
    private readonly IStorageEngine _engine;

    public KeyValueTypeTests()
    {
        _engine = CatDb.Database.CatDb.FromMemory();
    }

    public void Dispose() => _engine.Dispose();

    [Fact]
    public void Table_BoolKey_Works()
    {
        var t = _engine.OpenXTable<bool, string>("t");
        t[true] = "yes";
        t[false] = "no";
        t[true].Should().Be("yes");
        t[false].Should().Be("no");
    }

    [Fact]
    public void Table_ByteKey_Works()
    {
        var t = _engine.OpenXTable<byte, string>("t");
        t[0] = "zero";
        t[255] = "max";
        t[0].Should().Be("zero");
        t[255].Should().Be("max");
    }

    [Fact]
    public void Table_Int16Key_Works()
    {
        var t = _engine.OpenXTable<short, string>("t");
        t[short.MinValue] = "min";
        t[short.MaxValue] = "max";
        t[short.MinValue].Should().Be("min");
        t[short.MaxValue].Should().Be("max");
    }

    [Fact]
    public void Table_Int32Key_Works()
    {
        var t = _engine.OpenXTable<int, string>("t");
        t[int.MinValue] = "min";
        t[0] = "zero";
        t[int.MaxValue] = "max";
        t[0].Should().Be("zero");
    }

    [Fact]
    public void Table_Int64Key_Works()
    {
        var t = _engine.OpenXTable<long, string>("t");
        t[long.MinValue] = "min";
        t[long.MaxValue] = "max";
        t[long.MinValue].Should().Be("min");
    }

    [Fact]
    public void Table_UInt64Key_Works()
    {
        var t = _engine.OpenXTable<ulong, string>("t");
        t[ulong.MaxValue] = "max";
        t[ulong.MaxValue].Should().Be("max");
    }

    [Fact]
    public void Table_FloatKey_Works()
    {
        var t = _engine.OpenXTable<float, string>("t");
        t[1.5f] = "one-point-five";
        t[1.5f].Should().Be("one-point-five");
    }

    [Fact]
    public void Table_DoubleKey_Works()
    {
        var t = _engine.OpenXTable<double, string>("t");
        t[3.14] = "pi";
        t[3.14].Should().Be("pi");
    }

    [Fact]
    public void Table_DecimalKey_Works()
    {
        var t = _engine.OpenXTable<decimal, string>("t");
        t[1234567890.123456789m] = "big-decimal";
        t[1234567890.123456789m].Should().Be("big-decimal");
    }

    [Fact]
    public void Table_DateTimeKey_Works()
    {
        var t = _engine.OpenXTable<DateTime, string>("t");
        var dt = new DateTime(2025, 6, 15, 12, 0, 0, DateTimeKind.Utc);
        t[dt] = "noon";
        t[dt].Should().Be("noon");
    }

    [Fact]
    public void Table_TimeSpanKey_Works()
    {
        var t = _engine.OpenXTable<TimeSpan, string>("t");
        t[TimeSpan.FromHours(1)] = "one-hour";
        t[TimeSpan.FromHours(1)].Should().Be("one-hour");
    }

    [Fact]
    public void Table_StringKey_Works()
    {
        var t = _engine.OpenXTable<string, int>("t");
        t["alpha"] = 1;
        t["beta"] = 2;
        t["alpha"].Should().Be(1);
        t["beta"].Should().Be(2);
    }

    [Fact]
    public void Table_StringKey_LargeString_Works()
    {
        var t = _engine.OpenXTable<string, string>("t");
        var longKey = new string('x', 1000);
        t[longKey] = "long-value";
        t[longKey].Should().Be("long-value");
    }

    [Fact]
    public void Table_DecimalValue_RoundTrips()
    {
        var t = _engine.OpenXTable<int, decimal>("t");
        t[1] = decimal.MaxValue;
        t[2] = decimal.MinValue;
        t[3] = 0.000000000000000001m;
        _engine.Commit();
        t[1].Should().Be(decimal.MaxValue);
        t[2].Should().Be(decimal.MinValue);
        t[3].Should().Be(0.000000000000000001m);
    }

    [Fact]
    public void Table_StructKey_Works()
    {
        var t = _engine.OpenXTable<TestKey, string>("t");
        var key = new TestKey { Id = 42, Name = "foo" };
        t[key] = "bar";
        t[key].Should().Be("bar");
    }

    [Fact]
    public void Table_ComplexClassValue_Works()
    {
        var t = _engine.OpenXTable<long, Tick>("t");
        var tick = new Tick("GOOG", new DateTime(2025, 1, 1), 150.0, 150.1, 10, 20, "Test");
        t[1L] = tick;
        t[1L].Should().BeEquivalentTo(tick);
    }

    [Fact]
    public void Table_ByteArrayValue_Works()
    {
        var t = _engine.OpenXTable<int, byte[]>("t");
        var data = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        t[1] = data;
        t[1].Should().BeEquivalentTo(data);
    }
}
