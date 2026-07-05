// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System;
using System.Linq;
using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using FluentAssertions;
using Xunit;

namespace CatDb.Tests.Database;

/// <summary>
/// Regression coverage for three .NET-10 / type-normalization bugs:
///   1. Guid-typed schema fields threw AmbiguousMatchException (ToByteArray got a bool overload).
///   2. Enum-typed index fields threw InvalidCastException (Slots&lt;Enum,..&gt; vs normalized Slots&lt;int,..&gt;).
///   3. Nullable index fields threw NullReferenceException on the second distinct key.
/// Enum → underlying integral and Nullable&lt;T&gt; → T are now normalized on both the write and
/// query sides so index keys match the reopen-regenerated storage CLR type.
/// </summary>
public class EnumNullableGuidIndexTests
{
    public enum Color { Red, Green, Blue }

    public class GuidRow
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }

    public class EnumRow
    {
        public Color Color { get; set; }
        public int Age { get; set; }
    }

    public class NullDateRow
    {
        public DateTime? When { get; set; }
        public int Age { get; set; }
    }

    [Fact]
    public void Guid_Schema_Roundtrips()
    {
        using var e = CatDb.Database.CatDb.FromMemory();
        var t = e.OpenXTable<int, GuidRow>("g");
        var id = Guid.NewGuid();
        t.Replace(1, new GuidRow { Id = id, Name = "a" });
        e.Commit();

        t[1].Id.Should().Be(id);
        t[1].Name.Should().Be("a");
    }

    [Fact]
    public void Enum_NonUniqueIndex_Equality_Count_And_OrderBy()
    {
        using var e = CatDb.Database.CatDb.FromMemory();
        var t = e.OpenXTable<int, EnumRow>("en");
        t.CreateIndex("Color", x => x.Color, IndexType.NonUnique);

        t.Replace(1, new EnumRow { Color = Color.Red, Age = 10 });
        t.Replace(2, new EnumRow { Color = Color.Blue, Age = 20 });
        t.Replace(3, new EnumRow { Color = Color.Red, Age = 30 });
        e.Commit();

        t.Query(x => x.Color).Equal(Color.Red).Count().Should().Be(2);
        t.Query(x => x.Color).Equal(Color.Blue).Count().Should().Be(1);
        t.Query(x => x.Color).Equal(Color.Green).Count().Should().Be(0);

        // Range across enum ordinals (Red=0 .. Blue=2) then order by the indexed enum field.
        var byColor = t.Query(x => x.Color).Between(Color.Red, Color.Blue)
            .OrderBy(x => x.Color).Select(r => r.Value.Color).ToList();
        byColor.Should().Equal(Color.Red, Color.Red, Color.Blue);
    }

    [Fact]
    public void Enum_UniqueIndex_Works()
    {
        using var e = CatDb.Database.CatDb.FromMemory();
        var t = e.OpenXTable<int, EnumRow>("enu");
        t.CreateIndex("Color", x => x.Color, IndexType.Unique);

        t.Replace(1, new EnumRow { Color = Color.Red, Age = 1 });
        t.Replace(2, new EnumRow { Color = Color.Blue, Age = 2 });
        e.Commit();

        t.Query(x => x.Color).Equal(Color.Blue).Single().Key.Should().Be(2);
    }

    [Fact]
    public void NullableDateTime_Index_WithValuesAndNull()
    {
        using var e = CatDb.Database.CatDb.FromMemory();
        var t = e.OpenXTable<int, NullDateRow>("nd");
        t.CreateIndex("When", x => x.When, IndexType.NonUnique);

        var d2020 = new DateTime(2020, 1, 1);
        var d2021 = new DateTime(2021, 1, 1);
        t.Replace(1, new NullDateRow { When = d2020, Age = 1 });
        t.Replace(2, new NullDateRow { When = d2021, Age = 2 }); // 2nd distinct value (used to NRE)
        t.Replace(3, new NullDateRow { When = null, Age = 3 });  // null → default(DateTime)
        e.Commit();

        t.Query(x => x.When).Equal(d2020).Count().Should().Be(1);
        t.Query(x => x.When).Equal(d2021).Count().Should().Be(1);
        // null normalizes to DateTime.MinValue (default) — queryable, no crash.
        t.Query(x => x.When).Equal((DateTime?)DateTime.MinValue).Count().Should().Be(1);

        var asc = t.Query(x => x.When).OrderBy(x => x.When).Select(r => r.Key).ToList();
        asc.Should().Equal(3, 1, 2); // MinValue(null) < 2020 < 2021
    }

    [Fact]
    public void EqualNullLiteral_DoesNotDesyncPlanCacheSlots()
    {
        // Regression: QueryCompiler.Signature (the plan-cache key) is value-independent, but
        // Collect() used to SKIP adding a parameter slot when the literal was null — so a plan
        // compiled from a null-valued run had one fewer slot than a same-shape non-null run (or
        // vice versa), and ParameterizedContext.Parameter(slot) threw IndexOutOfRangeException on
        // whichever run came second. Exercise both orders against the SAME cached plan shape.
        using var e = CatDb.Database.CatDb.FromMemory();
        var t = e.OpenXTable<int, NullDateRow>("nulllit");
        t.CreateIndex("When", x => x.When, IndexType.NonUnique);

        var d = new DateTime(2020, 1, 1);
        t.Replace(1, new NullDateRow { When = d, Age = 1 });
        t.Replace(2, new NullDateRow { When = null, Age = 2 });
        e.Commit();

        // null first, then non-null, then null again — same query shape, alternating value null-ness.
        t.Query(x => x.When).Equal((DateTime?)null).Count().Should().Be(1);
        t.Query(x => x.When).Equal((DateTime?)d).Count().Should().Be(1);
        t.Query(x => x.When).Equal((DateTime?)null).Count().Should().Be(1);

        // Between with one null bound (Value2 slot) on the same cached shape.
        t.Query(x => x.When).Between((DateTime?)null, (DateTime?)d).Count().Should().Be(2);
    }

    [Fact]
    public void Enum_Index_SurvivesReopen()
    {
        var path = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(), $"catdb_enumidx_{Guid.NewGuid():N}.db");
        try
        {
            using (var e = CatDb.Database.CatDb.FromFile(path))
            {
                var t = e.OpenXTable<int, EnumRow>("en");
                t.CreateIndex("Color", x => x.Color, IndexType.NonUnique);
                t.Replace(1, new EnumRow { Color = Color.Red, Age = 1 });
                t.Replace(2, new EnumRow { Color = Color.Blue, Age = 2 });
                e.Commit();
            }

            // Reopen: the composite key CLR type is regenerated from the normalized DataType.
            using (var e = CatDb.Database.CatDb.FromFile(path))
            {
                var t = e.OpenXTable<int, EnumRow>("en");
                t.Replace(3, new EnumRow { Color = Color.Red, Age = 3 });
                e.Commit();
                t.Query(x => x.Color).Equal(Color.Red).Count().Should().Be(2);
            }
        }
        finally
        {
            if (System.IO.File.Exists(path)) System.IO.File.Delete(path);
        }
    }
}
