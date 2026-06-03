// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;

/// <summary>
/// Secondary index demo - shows unique, non-unique, composite, and IData-level indexing.
/// </summary>
static class SecondaryIndexDemo
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
        Console.WriteLine("═══ Secondary Index Demo ═══\n");

        using var engine = openEngine(true);

        // Open a regular table — no special method needed!
        var table = engine.OpenXTable<int, Customer>("demo_customers");

        // ── Create indexes directly on the table ──────────────────────────────
        Console.WriteLine("Creating indexes...");

        // Unique index on Email (via expression)
        table.CreateIndex("Email", c => c.Email, IndexType.Unique);

        // Non-unique index on City
        table.CreateIndex("City", c => c.City, IndexType.NonUnique);

        // Composite index on (City, Age) using member names
        table.CreateIndex("CityAge", new[] { "City", "Age" }, IndexType.NonUnique);

        Console.WriteLine("  Created: Email (unique), City (non-unique), CityAge (composite)\n");

        // ── Insert data ───────────────────────────────────────────────────────
        Console.WriteLine("Inserting 1000 customers...");
        var cities = new[] { "NYC", "London", "Tokyo", "Paris", "Berlin" };
        for (int i = 1; i <= 1000; i++)
        {
            table.Replace(i, new Customer
            {
                Email = $"user{i}@example.com",
                City = cities[i % cities.Length],
                Age = 20 + (i % 50),
                Name = $"Customer {i}",
            });
        }
        engine.Commit();
        Console.WriteLine("  Done.\n");

        // ── Unique index lookup ───────────────────────────────────────────────
        Console.WriteLine("Unique index lookup: Email = 'user42@example.com'");
        var results = table.Query(c => c.Email).Equals("user42@example.com").ToList();
        foreach (var kv in results)
            Console.WriteLine($"  Key={kv.Key}, Name={kv.Value.Name}, City={kv.Value.City}");

        // ── Non-unique index lookup ───────────────────────────────────────────────
        Console.WriteLine("\nNon-unique index lookup: City = 'Tokyo'");
        var tokyoResults = table.Query(c => c.City).Equals("Tokyo").Take(5).ToList();
        Console.WriteLine($"  Found {table.Query(c => c.City).Equals("Tokyo").Count()} total, showing first 5:");
        foreach (var kv in tokyoResults)
            Console.WriteLine($"  Key={kv.Key}, Name={kv.Value.Name}, Age={kv.Value.Age}");

        // ── Unique constraint enforcement ─────────────────────────────────────
        Console.WriteLine("\nTrying to insert duplicate email...");
        try
        {
            table.Replace(9999, new Customer { Email = "user42@example.com", City = "LA", Age = 30 });
            Console.WriteLine("  ERROR: Should have thrown!");
        }
        catch (UniqueIndexViolationException ex)
        {
            Console.WriteLine($"  Caught UniqueIndexViolationException: index='{ex.IndexName}'");
        }

        // ── Update maintenance ────────────────────────────────────────────────
        Console.WriteLine("\nUpdating customer 42's city from Tokyo to Berlin...");
        var c42 = table.Find(42);
        c42.City = "Berlin";
        table.Replace(42, c42);
        engine.Commit();

        var berlinCount = table.Query(c => c.City).Equals("Berlin").Count();
        Console.WriteLine($"  Berlin now has {berlinCount} customers");

        // ── Range search ──────────────────────────────────────────────────────
        Console.WriteLine("\nRange search: Email from 'user10@...' to 'user19@...'");
        var rangeResults = table.Query(c => c.Email)
            .AtLeast("user10@example.com").AtMost("user19@example.com").ToList();
        Console.WriteLine($"  Found {rangeResults.Count} records");

        // ── Delete maintenance ────────────────────────────────────────────────
        Console.WriteLine("\nDeleting customer 42...");
        table.Delete(42);
        engine.Commit();
        var exists = table.Query(c => c.Email).Equals("user42@example.com").Exists();
        Console.WriteLine($"  Email 'user42@example.com' exists in index: {exists}");

        // ── Portable (IData) table with manual index ──────────────────────────
        Console.WriteLine("\n─── IData/Portable table with manual index ───");
        var portable = engine.OpenXTable<long, Customer>("demo_portable_customers");
        // Use slot indices directly (Name=0, City=1 based on DataType ordering)
        // Or use member names which resolve via Descriptor.RecordMembers
        portable.Indexes.CreateIndex("PortableCity", new[] { "City" }, IndexType.NonUnique);

        for (long i = 1; i <= 100; i++)
            portable.Replace(i, new Customer { Email = $"p{i}@test.com", City = cities[i % 5], Age = (int)(20 + i % 30), Name = $"P{i}" });
        engine.Commit();

        var portableCityCount = portable.Indexes.CountByIndex("PortableCity",
            new CatDb.Data.Data<string>("London"));
        Console.WriteLine($"  'London' count via IData API: {portableCityCount}");

        // ── Bulk performance ──────────────────────────────────────────────────
        Console.WriteLine("\n─── Bulk insert: 100K records with 2 indexes ───");
        var bulkTable = engine.OpenXTable<long, Customer>("demo_bulk");
        bulkTable.CreateIndex("BulkEmail", c => c.Email, IndexType.Unique);
        bulkTable.CreateIndex("BulkCity", c => c.City, IndexType.NonUnique);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        for (long i = 1; i <= 100_000; i++)
        {
            bulkTable.Replace(i, new Customer
            {
                Email = $"bulk{i}@test.com",
                City = cities[i % 5],
                Age = (int)(20 + i % 50),
                Name = $"Bulk{i}",
            });
        }
        engine.Commit();
        sw.Stop();
        Console.WriteLine($"  100K inserts with 2 indexes: {sw.Elapsed.TotalSeconds:F2}s ({100_000 / sw.Elapsed.TotalSeconds:N0} ops/s)");

        // Cleanup
        engine.Delete("demo_customers");
        engine.Delete("demo_portable_customers");
        engine.Delete("demo_bulk");
        engine.Commit();

        Console.WriteLine("\n═══ Demo complete ═══");
    }
}
