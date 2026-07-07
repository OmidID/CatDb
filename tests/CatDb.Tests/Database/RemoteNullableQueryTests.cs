using System.Net;
using System.Net.Sockets;
using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.Extensions;
using CatDb.Remote;
using CatDb.General.Communication;
using FluentAssertions;

namespace CatDb.Tests.Database;

public class RemoteNullableQueryTests
{
    public class Item { public Guid Id { get; set; } public DateTime? ExpiresAt { get; set; } }

    private static int FreePort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0); l.Start();
        var p = ((IPEndPoint)l.LocalEndpoint).Port; l.Stop(); return p;
    }

    [Fact]
    public async Task Remote_NullableIndex_DirectApi_And_Query()
    {
        var port = FreePort();
        using var serverEngine = CatDb.Database.CatDb.FromMemory();
        await using var tcp = new TcpServer(port);
        var server = new StorageEngineServer(serverEngine, tcp, accessPolicy: null);
        await server.StartAsync();
        try
        {
            using var client = CatDb.Database.CatDb.FromNetwork("localhost", port, "default", "u", "p");
            var t = client.OpenXTable<long, Item>("items");
            t.CreateIndex("ExpiresAt", r => r.ExpiresAt, IndexType.NonUnique);

            var baseTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            for (long i = 0; i < 100; i++)
                t.Replace(i, new Item { Id = Guid.NewGuid(), ExpiresAt = i % 4 == 0 ? null : baseTime.AddDays(i) });
            client.Commit();

            t.Count().Should().Be(100);

            // direct index API
            var d5 = baseTime.AddDays(5);
            t.Indexes.FindByIndex("ExpiresAt", d5).Count().Should().Be(1);
            t.Indexes.CountByIndex("ExpiresAt", d5).Should().Be(1);

            // general query API (the path the stress test uses)
            t.Query(x => x.ExpiresAt).Equal(d5).Count().Should().Be(1);
            t.Query(x => x.ExpiresAt).AtLeast(baseTime.AddDays(10)).AtMost(baseTime.AddDays(50)).Count()
                .Should().Be(Enumerable.Range(0, 100).Count(i => i % 4 != 0 && baseTime.AddDays(i) >= baseTime.AddDays(10) && baseTime.AddDays(i) <= baseTime.AddDays(50)));
        }
        finally { await server.StopAsync(); }
    }
}
