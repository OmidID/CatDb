using CatDb.Database;
using CatDb.Tests.Data;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// Core CRUD tests for ITable&lt;TKey, TRecord&gt; using an in-memory engine.
/// Tests basic operations that were a source of crashes during net6 migration.
/// </summary>
public class TableCrudTests : IDisposable
{
    private readonly IStorageEngine _engine;

    public TableCrudTests()
    {
        _engine = CatDb.Database.CatDb.FromMemory();
    }

    public void Dispose() => _engine.Dispose();

    // ── Insert / Read ─────────────────────────────────────────────────────────

    [Fact]
    public void SetAndGet_Long_String_RoundTrips()
    {
        var table = _engine.OpenXTable<long, string>("t");
        table[1L] = "hello";
        table[1L].Should().Be("hello");
    }

    [Fact]
    public void SetAndGet_String_String_RoundTrips()
    {
        var table = _engine.OpenXTable<string, string>("t");
        table["key"] = "value";
        table["key"].Should().Be("value");
    }

    [Fact]
    public void SetAndGet_Int_ComplexClass_RoundTrips()
    {
        var table = _engine.OpenXTable<int, Tick>("t");
        var tick = new Tick("AAPL", new DateTime(2025, 1, 15, 9, 30, 0), 182.5, 182.51, 100, 200, "Bloomberg");
        table[1] = tick;
        var result = table[1];
        result.Should().BeEquivalentTo(tick);
    }

    [Fact]
    public void TryGet_ExistingKey_ReturnsTrueAndValue()
    {
        var table = _engine.OpenXTable<long, string>("t");
        table[42L] = "found";
        table.TryGet(42L, out var value).Should().BeTrue();
        value.Should().Be("found");
    }

    [Fact]
    public void TryGet_MissingKey_ReturnsFalse()
    {
        var table = _engine.OpenXTable<long, string>("t");
        table.TryGet(999L, out _).Should().BeFalse();
    }

    [Fact]
    public void Exists_ExistingKey_ReturnsTrue()
    {
        var table = _engine.OpenXTable<long, string>("t");
        table[1L] = "x";
        table.Exists(1L).Should().BeTrue();
    }

    [Fact]
    public void Exists_MissingKey_ReturnsFalse()
    {
        var table = _engine.OpenXTable<long, string>("t");
        table.Exists(999L).Should().BeFalse();
    }

    // ── Replace / InsertOrIgnore ──────────────────────────────────────────────

    [Fact]
    public void Replace_ExistingKey_UpdatesValue()
    {
        var table = _engine.OpenXTable<int, string>("t");
        table[1] = "original";
        table.Replace(1, "updated");
        table[1].Should().Be("updated");
    }

    [Fact]
    public void InsertOrIgnore_ExistingKey_DoesNotOverwrite()
    {
        var table = _engine.OpenXTable<int, string>("t");
        table[1] = "original";
        table.InsertOrIgnore(1, "ignored");
        table[1].Should().Be("original");
    }

    [Fact]
    public void InsertOrIgnore_NewKey_Inserts()
    {
        var table = _engine.OpenXTable<int, string>("t");
        table.InsertOrIgnore(1, "inserted");
        table[1].Should().Be("inserted");
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    [Fact]
    public void Delete_ExistingKey_RemovesEntry()
    {
        var table = _engine.OpenXTable<int, string>("t");
        table[1] = "x";
        table.Delete(1);
        table.Exists(1).Should().BeFalse();
    }

    [Fact]
    public void Delete_Range_RemovesExpectedEntries()
    {
        var table = _engine.OpenXTable<int, string>("t");
        for (var i = 1; i <= 10; i++) table[i] = i.ToString();

        table.Delete(3, 7); // removes keys 3..7 inclusive
        _engine.Commit();

        table.Exists(2).Should().BeTrue();
        table.Exists(3).Should().BeFalse();
        table.Exists(7).Should().BeFalse();
        table.Exists(8).Should().BeTrue();
    }

    [Fact]
    public void Clear_RemovesAllEntries()
    {
        var table = _engine.OpenXTable<int, string>("t");
        for (var i = 0; i < 50; i++) table[i] = i.ToString();
        table.Clear();
        _engine.Commit();
        table.Count().Should().Be(0);
    }

    // ── Count ─────────────────────────────────────────────────────────────────

    [Fact]
    public void Count_ReflectsInsertions()
    {
        var table = _engine.OpenXTable<int, string>("t");
        for (var i = 0; i < 10; i++) table[i] = i.ToString();
        _engine.Commit();
        table.Count().Should().Be(10);
    }

    [Fact]
    public void Count_AfterDelete_Decrements()
    {
        var table = _engine.OpenXTable<int, string>("t");
        for (var i = 0; i < 5; i++) table[i] = i.ToString();
        table.Delete(2);
        _engine.Commit();
        table.Count().Should().Be(4);
    }

    // ── Boundary rows ─────────────────────────────────────────────────────────

    [Fact]
    public void FirstRow_ReturnsSmallestKey()
    {
        var table = _engine.OpenXTable<int, string>("t");
        table[10] = "ten";
        table[1] = "one";
        table[5] = "five";
        _engine.Commit();
        table.FirstRow!.Value.Key.Should().Be(1);
    }

    [Fact]
    public void LastRow_ReturnsLargestKey()
    {
        var table = _engine.OpenXTable<int, string>("t");
        table[10] = "ten";
        table[1] = "one";
        table[5] = "five";
        _engine.Commit();
        table.LastRow!.Value.Key.Should().Be(10);
    }

    // ── Multiple tables isolation ─────────────────────────────────────────────

    [Fact]
    public void TwoTables_DataIsIsolated()
    {
        var t1 = _engine.OpenXTable<int, string>("table1");
        var t2 = _engine.OpenXTable<int, string>("table2");

        t1[1] = "from-t1";
        t2[1] = "from-t2";

        t1[1].Should().Be("from-t1");
        t2[1].Should().Be("from-t2");
    }

    [Fact]
    public void StorageEngine_Exists_FindsByName()
    {
        _engine.OpenXTable<int, string>("myTable");
        _engine.Exists("myTable").Should().BeTrue();
        _engine.Exists("nonexistent").Should().BeFalse();
    }

    [Fact]
    public void StorageEngine_Delete_RemovesTable()
    {
        _engine.OpenXTable<int, string>("toDelete");
        _engine.Commit();
        _engine.Delete("toDelete");
        _engine.Commit();
        _engine.Exists("toDelete").Should().BeFalse();
    }

    [Fact]
    public void StorageEngine_Rename_ChangesTableName()
    {
        _engine.OpenXTable<int, string>("oldName");
        _engine.Commit();
        _engine.Rename("oldName", "newName");
        _engine.Commit();
        _engine.Exists("newName").Should().BeTrue();
        _engine.Exists("oldName").Should().BeFalse();
    }
}
