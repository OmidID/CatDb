// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Diagnostics;
using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;

/// <summary>
/// Ordering demo — filter by one index/key, then ORDER BY another field. Shows each of the
/// engine's sort strategies and that the cross-index ones stream (bounded memory).
/// </summary>
static class SortDemo
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
        Console.WriteLine("═══ Ordering / Sort Demo ═══\n");

        using var engine = openEngine(true);
        var table = engine.OpenXTable<int, Customer>("sort_customers");

        table.CreateIndex("Email", c => c.Email, IndexType.Unique);
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);
        table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);
        table.CreateIndex("Name", c => c.Name, IndexType.NonUnique);
        table.CreateIndex("CityAge", new[] { "City", "Age" }, IndexType.NonUnique); // covering composite

        var cities = new[] { "berlin", "london", "nyc", "paris", "tokyo" };
        var names = new[] { "ann", "bob", "cara", "dan", "evan" };
        const int n = 200_000;
        Console.WriteLine($"Inserting {n:N0} customers...");
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < n; i++)
        {
            table.Replace(i, new Customer
            {
                Email = $"user{i:D7}@example.com",
                City = cities[i % cities.Length],
                Age = 18 + (i % 60),
                Name = names[i % names.Length],
            });
        }
        engine.Commit();
        Console.WriteLine($"  Done in {sw.Elapsed.TotalSeconds:F2}s\n");

        // 1) Primary-key order — pure streaming WTree scan, no buffer.
        Section("Key order (streaming): first 5 by key descending");
        foreach (var kv in table.Query().OrderByKeyDescending().Take(5))
            Console.WriteLine($"   #{kv.Key}  {kv.Value.Email}");

        // 2) Same-field index order — streamed straight from the index.
        Section("Index order (streaming): Email in [user0000100 .. user0000110] descending");
        foreach (var kv in table.Query(c => c.Email)
                     .AtLeast("user0000100@example.com").AtMost("user0000110@example.com")
                     .OrderByDescending(c => c.Email))
            Console.WriteLine($"   {kv.Value.Email}");

        // 3) Cross-index drive: filter by City, ORDER BY Age (different index) — streams sorted,
        //    City re-applied as a residual. Top 5 youngest Londoners.
        Section("Cross-index drive (streaming): City='london' ORDER BY Age — youngest 5");
        sw.Restart();
        foreach (var kv in table.Query(c => c.City).Equals("london")
                     .OrderBy(c => c.Age).Take(5))
            Console.WriteLine($"   #{kv.Key}  age={kv.Value.Age}  {kv.Value.Name}");
        Console.WriteLine($"   (returned in {sw.Elapsed.TotalMilliseconds:F1} ms — no full-set buffer)");

        // 4) Multi-key drive: leading index drives, equal-leading runs sorted by the rest.
        Section("Multi-key drive: City='nyc' ORDER BY Name, Age DESC — first 8");
        foreach (var kv in table.Query(c => c.City).Equals("nyc")
                     .OrderBy(c => c.Name).OrderByDescending(c => c.Age).Take(8))
            Console.WriteLine($"   {kv.Value.Name,-6} age={kv.Value.Age}  #{kv.Key}");

        // 5) Covering composite index: ORDER BY (City, Age) served entirely by the (City,Age) index.
        Section("Covering composite: key in [1000..200000] ORDER BY City, Age — first 8");
        foreach (var kv in table.Query().AtLeast(1000).AtMost(200_000)
                     .OrderBy(c => c.City).ThenBy(c => c.Age).Take(8))
            Console.WriteLine($"   {kv.Value.City,-7} age={kv.Value.Age}  #{kv.Key}");

        Console.WriteLine();
        Console.WriteLine("All ordered queries above (except the buffered fallback) stream with bounded memory.");
    }

    private static void Section(string title)
    {
        Console.WriteLine();
        Console.WriteLine($"── {title}");
    }
}
