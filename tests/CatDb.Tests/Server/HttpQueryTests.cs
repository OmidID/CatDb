// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Text.Json;
using CatDb.Database.Indexing;
using CatDb.Server.Services;
using FluentAssertions;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Primitives;
using Xunit;

namespace CatDb.Tests.Server;

/// <summary>
/// Drives the HTTP data-query stack end-to-end at the SERVICE layer (no TCP, no auth): the real
/// <see cref="QueryStringParser"/>/<see cref="JsonQueryParser"/> → <see cref="DataExplorerService.QueryTable"/>
/// → engine query planner → JSON path — the exact code the GET list endpoint and POST /query endpoint run.
/// Seeds an indexed object-record table and asserts the returned rows against a brute-force expectation for
/// each supported feature: flat AND filters, OR groups, between, prefix, ORDER BY, paging, and count.
/// </summary>
public sealed class HttpQueryTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"catdb_http_{Guid.NewGuid():N}");
    private readonly DatabaseHostService _host;
    private readonly DataExplorerService _explorer;

    private readonly record struct Person(long Key, string City, int Age, string Name);
    private readonly List<Person> _all = [];

    public HttpQueryTests()
    {
        Directory.CreateDirectory(_dir);
        var catalog = new SystemCatalogService(Path.Combine(_dir, "system.catdb"), NullLogger<SystemCatalogService>.Instance);
        _host = new DatabaseHostService(_dir, NullLogger<DatabaseHostService>.Instance, catalog);
        var tables = new TableManagementService(_host);
        _explorer = new DataExplorerService(_host);

        _host.CreateDatabase("qtest");
        var keySchema = Json("""{"type":"integer","format":"int64"}""");
        var valueSchema = Json("""
            {"type":"object","required":["City","Age","Name"],
             "properties":{"City":{"type":"string"},"Age":{"type":"integer","format":"int32"},"Name":{"type":"string"}}}
            """);
        tables.CreateTable("qtest", "people", keySchema, valueSchema);
        tables.CreateIndex("qtest", "people", "City", ["City"], IndexType.NonUnique);
        tables.CreateIndex("qtest", "people", "Age", ["Age"], IndexType.NonUnique);

        var cities = new[] { "nyc", "la", "berlin" };
        for (var i = 0; i < 300; i++)
        {
            var p = new Person(i, cities[i % 3], i % 50, $"n{i % 5}");
            _all.Add(p);
            _explorer.InsertRecord("qtest", "people", Json(i.ToString()),
                Json($$"""{"City":"{{p.City}}","Age":{{p.Age}},"Name":"{{p.Name}}"}"""));
        }
        _host.GetOrOpenDatabase("qtest").Commit();
    }

    public void Dispose()
    {
        (_host as IDisposable)?.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private static JsonElement Json(string s) => JsonDocument.Parse(s).RootElement;

    private static IQueryCollection Qc(params (string, string)[] kv) =>
        new QueryCollection(kv.ToDictionary(x => x.Item1, x => new StringValues(x.Item2)));

    /// <summary>Runs QueryTable and returns the serialized result (what the HTTP layer sends).</summary>
    private JsonElement Run(ParsedQuery spec)
        => Json(JsonSerializer.Serialize(_explorer.QueryTable("qtest", "people", spec)));

    private static List<long> Keys(JsonElement result)
        => result.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("key").GetInt64()).ToList();

    private static (string City, long Age) CityAge(JsonElement row)
        => (row.GetProperty("value").GetProperty("City").GetString()!,
            row.GetProperty("value").GetProperty("Age").GetInt64());

    [Fact]
    public void QueryString_AndFilter_OrderDesc_Count()
    {
        var spec = QueryStringParser.Parse(Qc(
            ("City", "eq:nyc"), ("Age", "gte:25"), ("order", "Age:desc"), ("limit", "1000"), ("count", "true")));

        var result = Run(spec);

        var rows = result.GetProperty("rows").EnumerateArray()
            .Select(CityAge).ToList();
        rows.Should().OnlyContain(r => r.City == "nyc" && r.Age >= 25);
        rows.Select(r => r.Age).Should().BeInDescendingOrder();

        var expected = _all.Count(p => p.City == "nyc" && p.Age >= 25);
        rows.Should().HaveCount(expected);
        result.GetProperty("total").GetInt64().Should().Be(expected);
    }

    [Fact]
    public void QueryString_BareValue_MeansEquals()
    {
        var spec = QueryStringParser.Parse(Qc(("City", "la"), ("limit", "1000")));
        var keys = Keys(Run(spec));
        keys.Should().BeEquivalentTo(_all.Where(p => p.City == "la").Select(p => p.Key));
    }

    [Fact]
    public void QueryString_Between_OnAge()
    {
        var spec = QueryStringParser.Parse(Qc(("Age", "between:10:20"), ("limit", "1000")));
        var rows = Run(spec).GetProperty("rows").EnumerateArray().Select(CityAge).ToList();
        rows.Should().OnlyContain(r => r.Age >= 10 && r.Age <= 20);
        rows.Should().HaveCount(_all.Count(p => p.Age >= 10 && p.Age <= 20));
    }

    [Fact]
    public void QueryString_OrGroup_AndedWithField()
    {
        // (City=nyc OR City=la) AND Age >= 40
        var spec = QueryStringParser.Parse(Qc(
            ("or", "(City:eq:nyc,City:eq:la)"), ("Age", "gte:40"), ("limit", "1000")));
        var keys = Keys(Run(spec)).OrderBy(k => k).ToList();
        var expected = _all.Where(p => (p.City == "nyc" || p.City == "la") && p.Age >= 40)
            .Select(p => p.Key).OrderBy(k => k).ToList();
        keys.Should().Equal(expected);
    }

    [Fact]
    public void QueryString_Prefix_OnName()
    {
        var spec = QueryStringParser.Parse(Qc(("Name", "prefix:n1"), ("limit", "1000")));
        var keys = Keys(Run(spec)).OrderBy(k => k).ToList();
        keys.Should().Equal(_all.Where(p => p.Name.StartsWith("n1")).Select(p => p.Key).OrderBy(k => k));
    }

    [Fact]
    public void QueryString_Paging_LimitOffset()
    {
        var ordered = _all.Where(p => p.City == "berlin").OrderBy(p => p.Key).Select(p => p.Key).ToList();
        var spec = QueryStringParser.Parse(Qc(
            ("City", "eq:berlin"), ("order", "$key:asc"), ("offset", "5"), ("limit", "10")));
        Keys(Run(spec)).Should().Equal(ordered.Skip(5).Take(10));
    }

    [Fact]
    public void JsonBody_NestedOrTree()
    {
        // (Age < 10 OR Age > 45) AND City = "nyc"
        var body = Json("""
            {"filter":{"and":[
                {"field":"City","op":"eq","value":"nyc"},
                {"or":[{"field":"Age","op":"lt","value":10},
                       {"field":"Age","op":"gt","value":45}]}]},
             "order":[{"field":"Age","desc":false},{"field":"$key"}],
             "take":1000,"count":true}
            """);
        var result = Run(JsonQueryParser.Parse(body));

        var rows = result.GetProperty("rows").EnumerateArray().Select(CityAge).ToList();
        rows.Should().OnlyContain(r => r.City == "nyc" && (r.Age < 10 || r.Age > 45));
        rows.Select(r => r.Age).Should().BeInAscendingOrder();

        var expected = _all.Count(p => p.City == "nyc" && (p.Age < 10 || p.Age > 45));
        rows.Should().HaveCount(expected);
        result.GetProperty("total").GetInt64().Should().Be(expected);
    }

    [Fact]
    public void JsonBody_Not()
    {
        // NOT (City = "nyc")  →  City in {la, berlin}
        var body = Json("""{"filter":{"not":{"field":"City","op":"eq","value":"nyc"}},"take":1000}""");
        var rows = Run(JsonQueryParser.Parse(body)).GetProperty("rows").EnumerateArray().Select(CityAge).ToList();
        rows.Should().OnlyContain(r => r.City != "nyc");
        rows.Should().HaveCount(_all.Count(p => p.City != "nyc"));
    }

    [Fact]
    public void EmptyQueryString_IsPlainBrowse()
    {
        QueryStringParser.Parse(Qc()).IsPlainBrowse.Should().BeTrue();
        QueryStringParser.Parse(Qc(("fromKey", "10"), ("toKey", "20"))).IsPlainBrowse.Should().BeTrue();
        QueryStringParser.Parse(Qc(("City", "eq:nyc"))).IsPlainBrowse.Should().BeFalse();
        QueryStringParser.Parse(Qc(("order", "Age"))).IsPlainBrowse.Should().BeFalse();
        QueryStringParser.Parse(Qc(("count", "true"))).IsPlainBrowse.Should().BeFalse();
    }

    [Fact]
    public void UnknownField_Returns_ArgumentException()
    {
        var spec = QueryStringParser.Parse(Qc(("Nope", "eq:x")));
        var act = () => _explorer.QueryTable("qtest", "people", spec);
        act.Should().Throw<ArgumentException>().WithMessage("*Nope*");
    }
}
