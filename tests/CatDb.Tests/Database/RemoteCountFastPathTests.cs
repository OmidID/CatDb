// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Net;
using System.Net.Sockets;
using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using CatDb.General.Communication;
using CatDb.Remote;
using FluentAssertions;

namespace CatDb.Tests.Database;

public class RemoteCountFastPathTests
{
    public class Product
    {
        public string Category { get; set; } = "";
        public int Stock { get; set; }
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
    public void Local_ChainedAndFilter_Count_MatchesManualRecount()
    {
        using var engine = CatDb.Database.CatDb.FromMemory();
        var t = engine.OpenXTable<int, Product>("p");
        t.CreateIndex("Category", p => p.Category, IndexType.NonUnique);
        t.CreateIndex("Stock", p => p.Stock, IndexType.NonUnique);

        var cats = new[] { "a", "b", "c" };
        var rng = new Random(1);
        for (var i = 0; i < 500; i++)
            t[i] = new Product { Category = cats[i % 3], Stock = rng.Next(0, 200) };
        engine.Commit();

        var fast = t.Query(p => p.Category).Equal("a").And(p => p.Stock).AtLeast(50).Count();
        var manual = t.Query(p => p.Category).Equal("a").Count(kv => kv.Value.Stock >= 50);

        fast.Should().Be(manual);
    }

    [Fact]
    public async Task Remote_ChainedAndFilter_Count_MatchesManualRecount()
    {
        var port = FreePort();
        using var serverEngine = CatDb.Database.CatDb.FromMemory();
        await using var tcp = new TcpServer(port);
        var server = new StorageEngineServer(serverEngine, tcp, accessPolicy: null);
        await server.StartAsync();

        try
        {
            using var client = CatDb.Database.CatDb.FromNetwork("localhost", port, "default", "u", "p");
            var t = client.OpenXTable<int, Product>("p");
            t.CreateIndex("Category", p => p.Category, IndexType.NonUnique);
            t.CreateIndex("Stock", p => p.Stock, IndexType.NonUnique);

            var cats = new[] { "a", "b", "c" };
            var rng = new Random(1);
            for (var i = 0; i < 500; i++)
                t[i] = new Product { Category = cats[i % 3], Stock = rng.Next(0, 200) };
            client.Commit();

            var fast = t.Query(p => p.Category).Equal("a").And(p => p.Stock).AtLeast(50).Count();
            var manual = t.Query(p => p.Category).Equal("a").Count(kv => kv.Value.Stock >= 50);
            var fetched = t.Query(p => p.Category).Equal("a").And(p => p.Stock).AtLeast(50).ToList().Count;

            fetched.Should().Be(manual, "ExecuteQuery row-fetch for the same AND filter should match manual recount");
            fast.Should().Be(manual);
        }
        finally
        {
            await server.StopAsync();
        }
    }

    [Fact]
    public async Task Remote_PrimaryKeyRangeScanCount_MatchesManualRecount()
    {
        // Regression for HighStressKeySearchService's Narrow.LtBwd case: table.Count(KeyQuery<long>
        // .LessThan(pivot)) / ScanCount with no client-side filter had no remote fast path — fell straight
        // to enumerating and counting every matching row over the wire. On a multi-million-row table that
        // turned a single logical "count" op into a scan that ran for minutes (the service showed 0 ops/sec
        // — not slow, STUCK on one call). Fixed via IRemoteScanTable.RangeCount / RangeCountCommand: a
        // single round trip evaluated server-side via the same O(leaves × log leafSize) leaf-index
        // arithmetic XTablePortable.ScanCount uses locally.
        var port = FreePort();
        using var serverEngine = CatDb.Database.CatDb.FromMemory();
        await using var tcp = new TcpServer(port);
        var server = new StorageEngineServer(serverEngine, tcp, accessPolicy: null);
        await server.StartAsync();

        try
        {
            using var client = CatDb.Database.CatDb.FromNetwork("localhost", port, "default", "u", "p");
            var t = client.OpenXTable<long, string>("ticks");

            const int n = 5_000;
            for (var i = 0L; i < n; i++) t[i] = $"v{i}";
            client.Commit();

            var pivot = n / 2;
            var fast = t.Count(KeyQuery<long>.LessThan(pivot));
            var manual = t.Forward().Count(kv => kv.Key < pivot);

            fast.Should().Be(manual);
            fast.Should().Be(pivot);
        }
        finally
        {
            await server.StopAsync();
        }
    }
}
