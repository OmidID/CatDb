// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Net;
using System.Net.Sockets;
using CatDb.Data;
using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using CatDb.General.Communication;
using CatDb.Remote;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// End-to-end secondary-index over the real TCP remote protocol: an in-process
/// <see cref="StorageEngineServer"/> backed by an in-memory engine, driven by a network client.
/// Exercises create/find/range/prefix/exists/count and ORDER BY — all serialized over the wire.
/// </summary>
public class RemoteIndexTests
{
    public class Customer
    {
        public string Email { get; set; } = "";
        public string City { get; set; } = "";
        public int Age { get; set; }
        public string Name { get; set; } = "";
    }

    public class NestedCustomer
    {
        public string Name { get; set; } = "";
        public Contact Contact { get; set; } = new();
    }

    public class Contact
    {
        public List<Phone> Phones { get; set; } = new();
        public string Email { get; set; } = "";
    }

    public class Phone
    {
        public string Type { get; set; } = "";
        public string Number { get; set; } = "";
    }

    private static int FreePort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        var port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    [Fact]
    public async Task Remote_NestedRecord_MemberNames_SurviveTheWire()
    {
        var port = FreePort();
        using var serverEngine = CatDb.Database.CatDb.FromMemory();
        await using var tcp = new TcpServer(port);
        var server = new StorageEngineServer(serverEngine, tcp, accessPolicy: null);
        await server.StartAsync();

        try
        {
            using (var client = CatDb.Database.CatDb.FromNetwork("localhost", port, "default", "u", "p"))
            {
                var table = client.OpenXTable<int, NestedCustomer>("nested");
                table.Replace(1, new NestedCustomer
                {
                    Name = "Omid",
                    Contact = new Contact
                    {
                        Email = "omid@example.com",
                        Phones = { new Phone { Type = "mobile", Number = "0617" } },
                    },
                });
                client.Commit();
            }

            // Inspect the map the server actually persisted (what the HTTP schema layer reads).
            var desc = serverEngine["nested"];
            var record = desc.RecordMemberMap;
            record.Should().NotBeNull();

            // Top-level names.
            record!.Names.Keys.Should().Contain(new[] { "Name", "Contact" });

            // Nested object: Contact -> { Phones, Email } (NOT Slot0/Slot1).
            var contactIdx = record.Names["Contact"];
            record.Children.Should().ContainKey(contactIdx);
            var contact = record.Children[contactIdx];
            contact.Names.Keys.Should().Contain(new[] { "Phones", "Email" });

            // Collection element: Phones[].{ Type, Number }.
            var phonesIdx = contact.Names["Phones"];
            contact.Children.Should().ContainKey(phonesIdx);
            var phoneElem = contact.Children[phonesIdx].Element;
            phoneElem.Should().NotBeNull();
            phoneElem!.Names.Keys.Should().Contain(new[] { "Type", "Number" });
        }
        finally
        {
            await server.StopAsync();
        }
    }

    [Fact]
    public async Task Remote_CursorPaging_AdaptivePages_NoDupsNoGaps()
    {
        var port = FreePort();
        using var serverEngine = CatDb.Database.CatDb.FromMemory();
        await using var tcp = new TcpServer(port);
        var server = new StorageEngineServer(serverEngine, tcp, accessPolicy: null);
        await server.StartAsync();

        try
        {
            using var client = CatDb.Database.CatDb.FromNetwork("localhost", port, "default", "u", "p");
            var table = client.OpenXTable<int, Customer>("customers");

            // > InitialPageCapacity (64) so the adaptive page growth + cursor-drop crosses boundaries.
            const int n = 1000;
            for (int i = 0; i < n; i++)
                table.Replace(i, new Customer { Email = $"u{i:D4}@x.com", City = "x", Age = i, Name = "n" });
            client.Commit();

            // Full forward scan (pages grow 64→512→… under the hood).
            table.Forward().Select(kv => kv.Key).Should()
                 .Equal(Enumerable.Range(0, n));

            // Cursor walk in tiny pages (the case that hung): each page is a fresh bounded Take(20).
            var seen = new List<int>();
            var first = true;
            var cursor = 0;
            while (true)
            {
                var page = (first
                    ? table.PageAfter(KeyQuery<int>.All(), take: 20)
                    : table.PageAfter(KeyQuery<int>.All(), afterKey: cursor, take: 20)).ToList();
                if (page.Count == 0) break;
                seen.AddRange(page.Select(kv => kv.Key));
                cursor = page[^1].Key;
                first = false;
            }

            seen.Should().Equal(Enumerable.Range(0, n));   // every key once, in order
        }
        finally
        {
            await server.StopAsync();
        }
    }

    [Fact]
    public async Task Remote_TakePushdown_Configurable_PagesAndBoundsCorrectly()
    {
        var port = FreePort();
        using var serverEngine = CatDb.Database.CatDb.FromMemory();
        await using var tcp = new TcpServer(port);
        var server = new StorageEngineServer(serverEngine, tcp, accessPolicy: null);
        await server.StartAsync();

        try
        {
            // Connection-level tuning: tiny pages so a Take(25) must page server-side (10+10+…)
            // through ForwardTake's drop-and-reseek logic.
            var opts = new RemoteScanOptions { InitialPageCapacity = 4, MaxPageCapacity = 10, PageGrowthFactor = 2 };
            using var client = (StorageEngineClient)CatDb.Database.CatDb.FromNetwork(
                "localhost", port, "default", "u", "p", opts);

            var table = client.OpenXTable<int, Customer>("customers");
            const int n = 500;
            for (int i = 0; i < n; i++)
                table.Replace(i, new Customer { Email = $"e{i:D3}", City = "c", Age = i, Name = "n" });
            client.Commit();

            // Bounded take far larger than MaxPageCapacity → server-side paging, exact bound.
            table.Query().Take(25).Select(kv => kv.Key).Should().Equal(Enumerable.Range(0, 25));

            // Cursor page after a key (exclusive-from skip on the fast path).
            table.PageAfter(KeyQuery<int>.All(), afterKey: 100, take: 25)
                 .Select(kv => kv.Key).Should().Equal(Enumerable.Range(101, 25));

            // Take past the end returns only what exists.
            table.Query().AtLeast(490).Take(50).Select(kv => kv.Key).Should()
                 .Equal(Enumerable.Range(490, 10));
        }
        finally
        {
            await server.StopAsync();
        }
    }

    [Fact]
    public async Task Remote_CoveringComposite_OrderBy_MaterializesTypedRecords()
    {
        var port = FreePort();
        using var serverEngine = CatDb.Database.CatDb.FromMemory();
        await using var tcp = new TcpServer(port);
        var server = new StorageEngineServer(serverEngine, tcp, accessPolicy: null);
        await server.StartAsync();

        try
        {
            using var client = CatDb.Database.CatDb.FromNetwork("localhost", port, "default", "u", "p");
            var table = client.OpenXTable<int, Customer>("customers");
            table.CreateIndex("CityAge", new[] { "City", "Age" }, IndexType.NonUnique);

            var cities = new[] { "berlin", "london", "nyc" };
            for (int i = 0; i < 90; i++)
                table.Replace(i, new Customer
                {
                    Email = $"u{i:D3}@x.com",
                    City = cities[i % cities.Length],
                    Age = i % 7,
                    Name = $"n{i % 4}",
                });
            client.Commit();

            // Covering-composite drive (TryCompositeDrive → CompositeScan): multi-key ORDER BY
            // with no equality prefix, covered by (City, Age). Over the wire the index scan returns
            // Data<Slots>; before the fix this threw InvalidCastException materializing TRecord.
            var rows = table.Query()
                .OrderBy(c => c.City)
                .OrderBy(c => c.Age)
                .Select(r => (r.Value.City, r.Value.Age))
                .ToList();

            rows.Should().HaveCount(90);
            var expected = rows.OrderBy(x => x.City, StringComparer.Ordinal).ThenBy(x => x.Age).ToList();
            rows.Should().Equal(expected);
        }
        finally
        {
            await server.StopAsync();
        }
    }

    [Fact]
    public async Task Remote_ServerSideException_InBatch_SurfacesRealMessage_NotCastError()
    {
        var port = FreePort();
        using var serverEngine = CatDb.Database.CatDb.FromMemory();
        await using var tcp = new TcpServer(port);
        var server = new StorageEngineServer(serverEngine, tcp, accessPolicy: null);
        await server.StartAsync();

        try
        {
            using var client = CatDb.Database.CatDb.FromNetwork("localhost", port, "default", "u", "p");
            var table = client.OpenXTable<int, Customer>("customers");
            table.CreateIndex("Email", c => c.Email, IndexType.Unique);

            table.Replace(1, new Customer { Email = "dup@x.com", City = "a", Age = 1, Name = "x" });
            // Second row reuses the unique Email → the server throws while draining the queued
            // batch; the failure is reported as an ExceptionCommand in the result batch.
            table.Replace(2, new Customer { Email = "dup@x.com", City = "b", Age = 2, Name = "y" });

            // A synchronous op flushes the batch. Before the fix this mis-cast the
            // ExceptionCommand → InvalidCastException; now it must surface the real message.
            Action flush = () => { _ = table.Count(); };

            flush.Should().Throw<Exception>()
                 .Where(e => !e.ToString().Contains("InvalidCastException"));
        }
        finally
        {
            await server.StopAsync();
        }
    }

    [Fact]
    public async Task Remote_SecondaryIndex_And_Ordering_EndToEnd()
    {
        var port = FreePort();
        using var serverEngine = CatDb.Database.CatDb.FromMemory();
        await using var tcp = new TcpServer(port);
        var server = new StorageEngineServer(serverEngine, tcp, accessPolicy: null);
        await server.StartAsync();

        try
        {
            using var client = CatDb.Database.CatDb.FromNetwork("localhost", port, "default", "u", "p");
            var table = client.OpenXTable<int, Customer>("customers");

            table.CreateIndex("Email", c => c.Email, IndexType.Unique);
            table.CreateIndex("City", c => c.City, IndexType.NonUnique);
            table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);
            table.CreateIndex("CityAge", new[] { "City", "Age" }, IndexType.NonUnique);

            var cities = new[] { "berlin", "london", "nyc" };
            for (int i = 0; i < 120; i++)
                table.Replace(i, new Customer
                {
                    Email = $"user{i:D3}@x.com",
                    City = cities[i % cities.Length],
                    Age = i % 10,
                    Name = $"n{i % 5}",
                });
            client.Commit();

            // Unique equality
            var byEmail = table.Query(c => c.Email).Equals("user042@x.com").ToList();
            byEmail.Should().ContainSingle().Which.Key.Should().Be(42);

            // Non-unique count + exists
            table.CountByIndex<int, Customer, string>("City", "nyc").Should().Be(40);
            table.Query(c => c.City).Equals("london").Exists().Should().BeTrue();

            // Range over the index
            var range = table.Query(c => c.Email)
                .AtLeast("user010@x.com").AtMost("user019@x.com").Count();
            range.Should().Be(10);

            // Descending index scan (backward over the wire)
            var topAges = table.Query(c => c.Age).Backward().Take(3).Select(r => r.Value.Age).ToList();
            topAges.Should().Equal(9, 9, 9);

            // Cross-index / prefix ORDER BY over the wire: City='nyc' ORDER BY Age (uses (City,Age))
            var nycByAge = table.Query(c => c.City).Equals("nyc")
                .OrderBy(c => c.Age).Select(r => r.Value.Age).ToList();
            nycByAge.Should().HaveCount(40);
            nycByAge.Should().BeInAscendingOrder();

            // Same, descending
            var nycByAgeDesc = table.Query(c => c.City).Equals("nyc")
                .OrderByDescending(c => c.Age).Select(r => r.Value.Age).ToList();
            nycByAgeDesc.Should().BeInDescendingOrder();
        }
        finally
        {
            await server.StopAsync();
        }
    }
}
