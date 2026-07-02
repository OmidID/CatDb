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
            table.QueryTake(KeyQuery<int>.All(), 25).Select(kv => kv.Key).Should().Equal(Enumerable.Range(0, 25));

            // Cursor page after a key (exclusive-from skip on the fast path).
            table.PageAfter(KeyQuery<int>.All(), afterKey: 100, take: 25)
                 .Select(kv => kv.Key).Should().Equal(Enumerable.Range(101, 25));

            // Take past the end returns only what exists.
            table.QueryTake(KeyQuery<int>.AtLeast(490), 50).Select(kv => kv.Key).Should()
                 .Equal(Enumerable.Range(490, 10));
        }
        finally
        {
            await server.StopAsync();
        }
    }

    [Fact]
    public async Task Remote_Builder_MultiIndex_Filter_Sort_OverTheWire()
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
            table.CreateIndex("City", c => c.City, IndexType.NonUnique);
            table.CreateIndex("Age", c => c.Age, IndexType.NonUnique);

            var cities = new[] { "berlin", "london", "nyc" };
            var all = new List<(int Key, string City, int Age, string Name)>();
            for (int i = 0; i < 200; i++)
            {
                var c = new Customer { Email = $"u{i:D3}@x.com", City = cities[i % 3], Age = i % 12, Name = $"n{i % 5}" };
                table.Replace(i, c);
                all.Add((i, c.City, c.Age, c.Name));
            }
            client.Commit();

            // The unified field builder now executes ON THE SERVER ENGINE over the wire:
            // City index ∩ Age index intersection + Name residual + ORDER BY, all server-side.
            var rows = table.Query(q => q.City).Equal("nyc")
                .And(q => q.Age).AtLeast(3).AtMost(9)
                .And(q => q.Name).Equal("n2")
                .OrderBy(o => o.Age).ThenBy(o => o.Email)
                .Select(kv => kv.Key).ToList();

            var expected = all
                .Where(r => r.City == "nyc" && r.Age >= 3 && r.Age <= 9 && r.Name == "n2")
                .OrderBy(r => r.Age).ThenBy(r => r.Key)
                .Select(r => r.Key).ToList();

            rows.Should().Equal(expected);

            // Filtered count over the wire.
            table.Query(q => q.City).Equal("london").And(q => q.Age).GreaterThan(5).Count()
                 .Should().Be(all.Count(r => r.City == "london" && r.Age > 5));

            // Key-range + field sort over the wire.
            var keyRange = table.Query().KeyBetween(50, 100).OrderByDescending(o => o.Age)
                .Select(kv => kv.Key).ToList();
            keyRange.Should().HaveCount(51);
            keyRange.Should().BeEquivalentTo(Enumerable.Range(50, 51));
        }
        finally
        {
            await server.StopAsync();
        }
    }

    [Fact]
    public async Task Remote_CompositeIndex_RangeScan_EngineLevel()
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
                    Email = $"u{i:D3}@x.com", City = cities[i % cities.Length], Age = i % 7, Name = $"n{i % 4}",
                });
            client.Commit();

            // Full composite-index range scan over the wire returns every row (server-side scan,
            // composite Slots records correctly transported and keyed).
            var keys = table.Indexes.FindByIndexRange("CityAge",
                    null, false, true, null, false, true, backward: false)
                .Select(kv => (int)kv.Key).OrderBy(k => k).ToList();

            keys.Should().Equal(Enumerable.Range(0, 90));
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

            // Unique equality (engine index seek over the wire).
            var byEmail = table.Indexes.FindByIndex("Email", "user042@x.com")
                .Select(kv => (int)kv.Key).ToList();
            byEmail.Should().ContainSingle().Which.Should().Be(42);

            // Non-unique count + exists.
            table.Indexes.CountByIndex("City", "nyc").Should().Be(40);
            table.Indexes.ExistsInIndex("City", "london").Should().BeTrue();

            // Range over the index (server-side range scan).
            var range = table.Indexes.FindByIndexRange("Email",
                "user010@x.com", true, true,
                "user019@x.com", true, true, backward: false).Count();
            range.Should().Be(10);

            // Descending index range scan (backward over the wire) — first 3 keys.
            var topAgeKeys = table.Indexes.FindByIndexRange("Age",
                    null, false, true, null, false, true, backward: true)
                .Take(3).Select(kv => (int)kv.Key).ToList();
            topAgeKeys.Should().HaveCount(3);

            // Composite (City,Age) prefix scan: City='nyc' rows, ordered by Age, over the wire.
            var nycKeys = table.Indexes.FindByIndexPrefix("CityAge", "nyc", 1, backward: false)
                .Select(kv => (int)kv.Key).ToList();
            nycKeys.Should().HaveCount(40);
        }
        finally
        {
            await server.StopAsync();
        }
    }
}
