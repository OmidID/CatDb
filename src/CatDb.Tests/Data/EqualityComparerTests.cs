// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;
using FluentAssertions;

namespace CatDb.Tests.Data;

/// <summary>
/// Tests for EqualityComparer&lt;T&gt; — used for hash-based lookups and set operations.
/// </summary>
public class EqualityComparerTests
{
    [Fact]
    public void Int32_EqualValues_AreEqual()
    {
        var cmp = new CatDb.Data.EqualityComparer<int>();
        cmp.Equals(42, 42).Should().BeTrue();
        cmp.Equals(1, 2).Should().BeFalse();
    }

    [Fact]
    public void Int32_GetHashCode_SameValueSameHash()
    {
        var cmp = new CatDb.Data.EqualityComparer<int>();
        cmp.GetHashCode(99).Should().Be(cmp.GetHashCode(99));
    }

    [Fact]
    public void String_EqualValues_AreEqual()
    {
        var cmp = new CatDb.Data.EqualityComparer<string>();
        cmp.Equals("hello", "hello").Should().BeTrue();
        cmp.Equals("hello", "world").Should().BeFalse();
    }

    [Fact]
    public void String_NullValues_DoNotThrow()
    {
        var cmp = new CatDb.Data.EqualityComparer<string>();
        var act = () => cmp.Equals(null!, null!);
        act.Should().NotThrow();
    }

    [Fact]
    public void Double_EqualValues_AreEqual()
    {
        var cmp = new CatDb.Data.EqualityComparer<double>();
        cmp.Equals(3.14, 3.14).Should().BeTrue();
        cmp.Equals(1.0, 2.0).Should().BeFalse();
    }

    [Fact]
    public void Decimal_EqualValues_AreEqual()
    {
        var cmp = new CatDb.Data.EqualityComparer<decimal>();
        cmp.Equals(100.99m, 100.99m).Should().BeTrue();
        cmp.Equals(1.0m, 2.0m).Should().BeFalse();
    }

    [Fact]
    public void DateTime_EqualValues_AreEqual()
    {
        var cmp = new CatDb.Data.EqualityComparer<DateTime>();
        var d = new DateTime(2024, 6, 15);
        cmp.Equals(d, d).Should().BeTrue();
        cmp.Equals(d, d.AddDays(1)).Should().BeFalse();
    }

    [Fact]
    public void Guid_EqualValues_AreEqual()
    {
        var cmp = new CatDb.Data.EqualityComparer<Guid>();
        var g = Guid.NewGuid();
        cmp.Equals(g, g).Should().BeTrue();
        cmp.Equals(g, Guid.NewGuid()).Should().BeFalse();
    }

    [Fact]
    public void ByteArray_EqualContents_AreEqual()
    {
        var cmp = new CatDb.Data.EqualityComparer<byte[]>();
        cmp.Equals(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 }).Should().BeTrue();
        cmp.Equals(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 4 }).Should().BeFalse();
    }

    [Fact]
    public void Struct_EqualValues_AreEqual()
    {
        var cmp = new CatDb.Data.EqualityComparer<TestKey>();
        var a = new TestKey { Id = 1, Name = "test" };
        var b = new TestKey { Id = 1, Name = "test" };
        var c = new TestKey { Id = 2, Name = "test" };
        cmp.Equals(a, b).Should().BeTrue();
        cmp.Equals(a, c).Should().BeFalse();
    }

    [Fact]
    public void Struct_GetHashCode_EqualValuesSameHash()
    {
        var cmp = new CatDb.Data.EqualityComparer<TestKey>();
        var a = new TestKey { Id = 5, Name = "foo" };
        var b = new TestKey { Id = 5, Name = "foo" };
        cmp.GetHashCode(a).Should().Be(cmp.GetHashCode(b));
    }
}
