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

    private static int FreePort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        var port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
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
