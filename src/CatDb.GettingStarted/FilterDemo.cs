// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Diagnostics;
using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;

/// <summary>
/// Filtering demo — the single field-oriented query builder. Multiple predicates across different
/// fields are chained with <c>.And(...)</c> and ANDed; the <b>engine</b> resolves each to an index,
/// intersects the indexes by primary key, evaluates non-indexed fields as a residual, and orders the
/// result. Nothing is filtered in .NET after the fact.
/// </summary>
static class FilterDemo
{
    public class Customer
    {
        public string Email { get; set; } = "";
        public string City { get; set; } = "";
        public int Age { get; set; }
        public string Name { get; set; } = "";
    }

    public static void Run(Func<bool, IStorageEngine> openEngine)
    {
        Console.WriteLine("═══ Filter Demo — engine-level multi-index queries ═══\n");

        using var engine = openEngine(true);
        var table = engine.OpenXTable<int, Customer>("filter_customers");

        table.CreateIndex("City", c => c.City, IndexType.NonUnique);
        table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);
        table.CreateIndex("Name", c => c.Name, IndexType.NonUnique);

        var cities = new[] { "berlin", "london", "nyc", "paris", "tokyo" };
        var names = new[] { "ann", "bob", "cara", "dan", "evan" };
        const int n = 200_000;
        Console.WriteLine($"Inserting {n:N0} customers...");
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < n; i++)
            table.Replace(i, new Customer
            {
                Email = $"user{i:D7}@example.com",
                City = cities[i % cities.Length],
                Age = 18 + (i % 60),
                Name = names[i % names.Length],
            });
        engine.Commit();
        Console.WriteLine($"  Done in {sw.Elapsed.TotalSeconds:F2}s\n");

        // 1) Two indexes intersected at the engine: City index ∩ Age index.
        Section("Multi-index AND: City='london' AND Age in [30..40] — first 8");
        sw.Restart();
        foreach (var kv in table.Query(q => q.City).Equal("london")
                     .And(q => q.Age).AtLeast(30).AtMost(40)
                     .Take(8))
            Console.WriteLine($"   #{kv.Key}  city={kv.Value.City}  age={kv.Value.Age}");
        Console.WriteLine($"   (engine intersected two indexes in {sw.Elapsed.TotalMilliseconds:F1} ms)");

        // 2) Three predicates: two indexed (City, Name) + one residual (Email prefix, no index).
        Section("Indexed ∩ indexed + residual: City='nyc' AND Name='cara' AND Email starts 'user000001'");
        foreach (var kv in table.Query(q => q.City).Equal("nyc")
                     .And(q => q.Name).Equal("cara")
                     .And(q => q.Email).StartsWith("user000001")
                     .Take(8))
            Console.WriteLine($"   #{kv.Key}  {kv.Value.Email}  {kv.Value.Name}");

        // 3) Filter + multi-key ORDER BY, engine-ordered.
        Section("Filter then sort: City='paris' ORDER BY Age, Name DESC — first 8");
        foreach (var kv in table.Query(q => q.City).Equal("paris")
                     .OrderBy(o => o.Age).ThenByDescending(o => o.Name)
                     .Take(8))
            Console.WriteLine($"   #{kv.Key}  age={kv.Value.Age}  {kv.Value.Name}");

        // 4) Filtered count — engine intersects then counts.
        Section("Filtered count: City='tokyo' AND Age in [25..35]");
        sw.Restart();
        var cnt = table.Query(q => q.City).Equal("tokyo").And(q => q.Age).AtLeast(25).AtMost(35).Count();
        Console.WriteLine($"   {cnt:N0} matches  (in {sw.Elapsed.TotalMilliseconds:F1} ms)");

        // 5) Primary-key range + field sort.
        Section("Key range + sort: key in [1000..2000] ORDER BY City — first 8");
        foreach (var kv in table.Query()
                     .KeyBetween(1000, 2000)
                     .OrderBy(o => o.City).ThenBy(o => o.Age)
                     .Take(8))
            Console.WriteLine($"   #{kv.Key}  {kv.Value.City}  age={kv.Value.Age}");

        Console.WriteLine();
        Console.WriteLine("Every predicate is structured (field, op, value): the engine chooses indexes,");
        Console.WriteLine("intersects them by primary key, and orders the result — no post-filtering in .NET.");
    }

    private static void Section(string title)
    {
        Console.WriteLine();
        Console.WriteLine($"── {title}");
    }
}
