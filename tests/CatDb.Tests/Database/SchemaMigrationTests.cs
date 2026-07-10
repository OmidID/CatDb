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

/// <summary>
/// Automatic record-schema migration: reopening a table with a record type that gained, lost or
/// reordered properties must migrate the stored rows (name-based slot remap; new members default,
/// removed members drop) instead of throwing. Covers local file reopen, remote open, index
/// survival via CreateIndex backfill, and the non-migratable diagnostics.
/// </summary>
public class SchemaMigrationTests : IDisposable
{
    private readonly string _filePath;

    public SchemaMigrationTests()
    {
        _filePath = Path.Combine(Path.GetTempPath(), $"catdb_migr_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_filePath))
            File.Delete(_filePath);
    }

    public class PersonV1
    {
        public string Name { get; set; } = "";
        public int Age { get; set; }
    }

    // V1 + appended property
    public class PersonV2Added
    {
        public string Name { get; set; } = "";
        public int Age { get; set; }
        public bool Active { get; set; }
    }

    // V1 + property added in the MIDDLE (shifts slots — only name-based mapping survives this)
    public class PersonV2Middle
    {
        public string Name { get; set; } = "";
        public string City { get; set; } = "";
        public int Age { get; set; }
    }

    // V1 with a property removed and another added
    public class PersonV2Mixed
    {
        public int Age { get; set; }
        public Guid? Tag { get; set; }
    }

    // V1 with Age's type changed — NOT migratable
    public class PersonV2TypeChanged
    {
        public string Name { get; set; } = "";
        public string Age { get; set; } = "";
    }

    [Fact]
    public void AddProperty_AtEnd_MigratesAndKeepsData()
    {
        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV1>("people");
            t[1L] = new PersonV1 { Name = "Ada", Age = 36 };
            t[2L] = new PersonV1 { Name = "Alan", Age = 41 };
            engine.Commit();
        }

        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV2Added>("people");
            t.Count().Should().Be(2);
            t[1L].Name.Should().Be("Ada");
            t[1L].Age.Should().Be(36);
            t[1L].Active.Should().BeFalse(); // new member → default
            engine.Commit();
        }

        // And the migrated data persists across another reopen.
        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV2Added>("people");
            t[2L].Name.Should().Be("Alan");
        }
    }

    [Fact]
    public void AddProperty_InMiddle_RemapsByName()
    {
        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV1>("people");
            t[1L] = new PersonV1 { Name = "Ada", Age = 36 };
            engine.Commit();
        }

        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV2Middle>("people");
            var p = t[1L];
            p.Name.Should().Be("Ada");
            p.City.Should().BeNullOrEmpty();  // inserted member → default
            p.Age.Should().Be(36);            // slot SHIFTED but name-mapped
        }
    }

    [Fact]
    public void RemoveAndAddProperties_DropsOldKeepsMatched()
    {
        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV1>("people");
            t[1L] = new PersonV1 { Name = "Ada", Age = 36 };
            engine.Commit();
        }

        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV2Mixed>("people");
            var p = t[1L];
            p.Age.Should().Be(36);     // matched by name
            p.Tag.Should().BeNull();   // new Guid? member → default
        }
    }

    [Fact]
    public void ChangeMemberType_ThrowsDiagnostic()
    {
        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV1>("people");
            t[1L] = new PersonV1 { Name = "Ada", Age = 36 };
            engine.Commit();
        }

        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var act = () => engine.OpenXTable<long, PersonV2TypeChanged>("people");
            act.Should().Throw<ArgumentException>()
                .WithMessage("*member 'Age' changed type*");
        }
    }

    [Fact]
    public void Migration_WithIndexes_IndexRebuiltViaCreateIndex()
    {
        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV1>("people");
            t.CreateIndex("Name", x => x.Name, IndexType.NonUnique);
            for (var i = 0; i < 50; i++)
                t[i] = new PersonV1 { Name = $"p{i % 5}", Age = i };
            engine.Commit();
        }

        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV2Added>("people"); // migrates
            t.CreateIndex("Name", x => x.Name, IndexType.NonUnique);  // recreates + backfills
            t.Query(x => x.Name).Equal("p3").Count().Should().Be(10);
            t.Query(x => x.Name).Equal("p3").ToList().Should().OnlyContain(kv => kv.Value.Name == "p3");
        }
    }

    [Fact]
    public void IndexData_SurvivesReopen_WithoutMigration()
    {
        // Index-table locators are now reused across restarts (and enrolled in the engine map for
        // commit flushing): re-registering the index on reopen must NOT wipe it, and it must work
        // immediately — this used to leave a silently EMPTY index after every process restart.
        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV1>("people");
            t.CreateIndex("Name", x => x.Name, IndexType.NonUnique);
            for (var i = 0; i < 30; i++)
                t[i] = new PersonV1 { Name = $"p{i % 3}", Age = i };
            engine.Commit();
        }

        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV1>("people");
            t.CreateIndex("Name", x => x.Name, IndexType.NonUnique);
            t.Query(x => x.Name).Equal("p1").Count().Should().Be(10);
        }
    }

    [Fact]
    public void CreateIndex_OnExistingRows_Backfills()
    {
        using var engine = CatDb.Database.CatDb.FromMemory();
        var t = engine.OpenXTable<long, PersonV1>("people");
        for (var i = 0; i < 20; i++)
            t[i] = new PersonV1 { Name = $"p{i % 2}", Age = i };

        // Index created AFTER the data exists must see it.
        t.CreateIndex("Name", x => x.Name, IndexType.NonUnique);
        t.Query(x => x.Name).Equal("p0").Count().Should().Be(10);
    }

    [Fact]
    public void CreateIndex_SameDefinitionTwice_IsIdempotent()
    {
        using var engine = CatDb.Database.CatDb.FromMemory();
        var t = engine.OpenXTable<long, PersonV1>("people");
        t[1] = new PersonV1 { Name = "Ada", Age = 36 };

        t.CreateIndex("Name", x => x.Name, IndexType.NonUnique);
        // Second identical registration (retried startup / ensure pattern) must be a no-op…
        var act = () => t.CreateIndex("Name", x => x.Name, IndexType.NonUnique);
        act.Should().NotThrow();
        t.Query(x => x.Name).Equal("Ada").Count().Should().Be(1);

        // …but a CONFLICTING definition under the same name still throws.
        var conflict = () => t.CreateIndex("Name", x => x.Age, IndexType.NonUnique);
        conflict.Should().Throw<InvalidOperationException>()
            .WithMessage("*different definition*");
    }

    [Fact]
    public void ReusedIndexTable_OutOfStepWithTable_IsRebuilt()
    {
        // Out-of-step persisted index: rows written while NO index was registered (run 2 below —
        // the manager only maintains indexes it knows about), then the index re-registered on a
        // later open. The reuse path must detect the entry-count/row-count drift and rebuild —
        // trusting the stale index would silently hide the run-2 rows from index queries.
        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV1>("people");
            t.CreateIndex("Name", x => x.Name, IndexType.NonUnique);
            for (var i = 0; i < 20; i++)
                t[i] = new PersonV1 { Name = $"p{i % 2}", Age = i };
            engine.Commit();
        }

        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV1>("people");
            for (var i = 20; i < 30; i++) // no index registered → index table not maintained
                t[i] = new PersonV1 { Name = $"p{i % 2}", Age = i };
            engine.Commit();
        }

        using (var engine = CatDb.Database.CatDb.FromFile(_filePath))
        {
            var t = engine.OpenXTable<long, PersonV1>("people");
            t.CreateIndex("Name", x => x.Name, IndexType.NonUnique);
            t.Query(x => x.Name).Equal("p1").Count().Should().Be(15); // 10 + 5 from run 2
        }
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
    public async Task Remote_SchemaChange_MigratesOnOpen()
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
                var t = client.OpenXTable<long, PersonV1>("people");
                t.Replace(1L, new PersonV1 { Name = "Ada", Age = 36 });
                client.Commit();
            }

            // New client, evolved entity: server must migrate, not throw.
            using (var client = CatDb.Database.CatDb.FromNetwork("localhost", port, "default", "u", "p"))
            {
                var t = client.OpenXTable<long, PersonV2Middle>("people");
                var p = t.Find(1L);
                p.Should().NotBeNull();
                p!.Name.Should().Be("Ada");
                p.Age.Should().Be(36);
                p.City.Should().BeNullOrEmpty();

                t.Replace(2L, new PersonV2Middle { Name = "Alan", City = "London", Age = 41 });
                client.Commit();

                t.Find(2L)!.City.Should().Be("London");
            }
        }
        finally
        {
            await server.StopAsync();
        }
    }
}
