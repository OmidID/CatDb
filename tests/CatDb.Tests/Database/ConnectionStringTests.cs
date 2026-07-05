// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Net;
using System.Net.Sockets;
using CatDb.Database;
using CatDb.General.Communication;
using CatDb.Remote;
using CatDb.Storage;
using FluentAssertions;

namespace CatDb.Tests.Database;

/// <summary>
/// <see cref="CatDb.Database.CatDb.FromConnectionString"/> / <c>FromConnectionStringAsync</c>: the
/// ADO.NET-style <c>Key=Value;...</c> parser that dispatches to the File/Memory/Network providers.
/// Covers provider inference, key aliases, DatabaseOptions/RemoteScanOptions tuning, and malformed-input
/// rejection. Tuning is verified through observable side effects (e.g. CommitMode picks the .wal vs
/// .oplog file) since <see cref="DatabaseOptions"/> itself isn't exposed off <see cref="IStorageEngine"/>.
/// </summary>
public class ConnectionStringTests : IDisposable
{
    private readonly List<string> _tempFiles = new();

    public void Dispose()
    {
        foreach (var f in _tempFiles)
            if (File.Exists(f)) File.Delete(f);
    }

    private string NewTempFile()
    {
        var file = Path.Combine(Path.GetTempPath(), $"catdb_constr_{Guid.NewGuid():N}.db");
        _tempFiles.Add(file);
        _tempFiles.Add(file + ".wal");
        _tempFiles.Add(file + ".oplog");
        return file;
    }

    private static int FreePort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        var port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    // ---- Memory provider ----

    [Fact]
    public void Memory_ExplicitProvider_RoundtripsData()
    {
        using var e = CatDb.Database.CatDb.FromConnectionString("Provider=Memory");
        var t = e.OpenXTable<int, string>("t");
        t[1] = "a";
        e.Commit();
        t[1].Should().Be("a");
    }

    [Fact]
    public void Memory_InferredWhenNoProviderPathOrHost()
    {
        using var e = CatDb.Database.CatDb.FromConnectionString("UseNativeLeafStorage=false");
        var t = e.OpenXTable<int, string>("t");
        t[1] = "a";
        t[1].Should().Be("a");
    }

    [Theory]
    [InlineData("Provider=Memory")]
    [InlineData("Provider=MEM")]
    [InlineData("provider=inmemory")]
    public void Memory_ProviderAliases_AreCaseInsensitive(string connectionString)
    {
        using var e = CatDb.Database.CatDb.FromConnectionString(connectionString);
        e.OpenXTable<int, string>("t").Should().NotBeNull();
    }

    // ---- File provider ----

    [Fact]
    public void File_InferredFromPath_PersistsAcrossReopen()
    {
        var file = NewTempFile();
        var cs = $"Path={file}";

        using (var e = CatDb.Database.CatDb.FromConnectionString(cs))
        {
            var t = e.OpenXTable<int, string>("t");
            t[1] = "a";
            e.Commit();
        }

        using var reopened = CatDb.Database.CatDb.FromConnectionString(cs);
        reopened.OpenXTable<int, string>("t")[1].Should().Be("a");
    }

    [Fact]
    public void File_CommitMode_WriteAheadLog_RoundtripsWithoutOpLog()
    {
        // WalHeap.Commit() writes the .wal burst, checkpoints into the heap, then deletes the .wal file —
        // all inline within the same Commit() call — so the WAL is transient by design (it only survives
        // a crash mid-commit) and asserting its presence right after a clean Commit() is not meaningful.
        // What IS observable: this mode never produces an .oplog (that's TransactionLog-only), and data
        // still round-trips through a reopen.
        var file = NewTempFile();
        var cs = $"Provider=File;Path={file};CommitMode=WriteAheadLog";

        using (var e = CatDb.Database.CatDb.FromConnectionString(cs))
        {
            var t = e.OpenXTable<int, string>("t");
            t[1] = "a";
            e.Commit();
            File.Exists(file + ".oplog").Should().BeFalse();
        }

        using var reopened = CatDb.Database.CatDb.FromConnectionString(cs);
        reopened.OpenXTable<int, string>("t")[1].Should().Be("a");
    }

    [Fact]
    public void File_CommitMode_TransactionLog_CreatesOpLogFile()
    {
        var file = NewTempFile();
        using var e = CatDb.Database.CatDb.FromConnectionString($"Provider=File;Path={file};CommitMode=TransactionLog");
        e.OpenXTable<int, string>("t")[1] = "a";
        e.Commit();

        File.Exists(file + ".oplog").Should().BeTrue();
        File.Exists(file + ".wal").Should().BeFalse();
    }

    [Fact]
    public void File_UseCompressionAndAllocationStrategyAliases_Roundtrip()
    {
        var file = NewTempFile();
        // "Compression"/"Strategy" are the short aliases for UseCompression/AllocationStrategy — this
        // proves both parse and actually feed the Heap ctor (a compressed InPlace file still round-trips).
        var cs = $"Path={file};Compression=true;Strategy=FromTheCurrentBlock;CommitMode=InPlace";

        using (var e = CatDb.Database.CatDb.FromConnectionString(cs))
        {
            var t = e.OpenXTable<int, string>("t");
            t[1] = "compressed";
            e.Commit();
        }

        using var reopened = CatDb.Database.CatDb.FromConnectionString(cs);
        reopened.OpenXTable<int, string>("t")[1].Should().Be("compressed");
    }

    [Fact]
    public void File_ByteSizeSuffix_IsAcceptedAndDataSurvivesReopen()
    {
        var file = NewTempFile();
        var cs = $"Provider=File;Path={file};CommitMode=TransactionLog;CheckpointLogSizeBytes=8MB;CacheSizeBytes=256MB";

        using (var e = CatDb.Database.CatDb.FromConnectionString(cs))
        {
            var t = e.OpenXTable<int, string>("t");
            t[1] = "sized";
            e.Commit();
        }

        using var reopened = CatDb.Database.CatDb.FromConnectionString(cs);
        reopened.OpenXTable<int, string>("t")[1].Should().Be("sized");
    }

    // ---- Network provider ----

    [Fact]
    public async Task Network_ExplicitProvider_RoundtripsOverRealTcp()
    {
        var port = FreePort();
        using var serverEngine = CatDb.Database.CatDb.FromMemory();
        await using var tcp = new TcpServer(port);
        var server = new StorageEngineServer(serverEngine, tcp, accessPolicy: null);
        await server.StartAsync();

        try
        {
            using var client = CatDb.Database.CatDb.FromConnectionString(
                $"Provider=Network;Host=localhost;Port={port};Database=default;User Id=u;Password=p");
            var t = client.OpenXTable<int, string>("t");
            t[1] = "remote";
            client.Commit();
            t[1].Should().Be("remote");
        }
        finally
        {
            await server.StopAsync();
        }
    }

    [Fact]
    public async Task Network_ConnectionStringAsync_Connects()
    {
        // NOTE: a client produced via ConnectAsync (StartAsync under the hood) puts the underlying
        // ClientConnection in async-send mode; IStorageEngine's synchronous surface (OpenXTable, Commit,
        // ...) always calls the sync Execute -> SendSync path, which throws "not in sync mode" for such a
        // client. That's a pre-existing limitation of StorageEngineClient.ConnectAsync/FromNetworkAsync,
        // not something introduced here — FromConnectionStringAsync intentionally mirrors FromNetworkAsync
        // exactly. This test only proves the connection-string parse + async connect handshake succeeds.
        var port = FreePort();
        using var serverEngine = CatDb.Database.CatDb.FromMemory();
        await using var tcp = new TcpServer(port);
        var server = new StorageEngineServer(serverEngine, tcp, accessPolicy: null);
        await server.StartAsync();

        try
        {
            using var client = await CatDb.Database.CatDb.FromConnectionStringAsync(
                $"Host=localhost;Port={port};Database=default;User Id=u;Password=p");
            client.Should().NotBeNull();
        }
        finally
        {
            await server.StopAsync();
        }
    }

    [Fact]
    public async Task Network_ScanAndClientTuning_AreAppliedFromConnectionString()
    {
        var port = FreePort();
        using var serverEngine = CatDb.Database.CatDb.FromMemory();
        await using var tcp = new TcpServer(port);
        var server = new StorageEngineServer(serverEngine, tcp, accessPolicy: null);
        await server.StartAsync();

        try
        {
            using var client = (StorageEngineClient)CatDb.Database.CatDb.FromConnectionString(
                $"Provider=Network;Host=localhost;Port={port};InitialPageCapacity=16;MaxPageCapacity=256;PageGrowthFactor=4;WriteBatchCapacity=64;CacheSize=99");

            client.ScanOptions.InitialPageCapacity.Should().Be(16);
            client.ScanOptions.MaxPageCapacity.Should().Be(256);
            client.ScanOptions.PageGrowthFactor.Should().Be(4);
            client.WriteBatchCapacity.Should().Be(64);
        }
        finally
        {
            await server.StopAsync();
        }
    }

    // ---- Error handling ----

    [Fact]
    public void EmptyConnectionString_Throws()
    {
        var act = () => CatDb.Database.CatDb.FromConnectionString("");
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void MalformedSegment_WithoutEquals_Throws()
    {
        var act = () => CatDb.Database.CatDb.FromConnectionString("Provider=Memory;ThisHasNoEquals");
        act.Should().Throw<FormatException>();
    }

    [Fact]
    public void UnknownProvider_Throws()
    {
        var act = () => CatDb.Database.CatDb.FromConnectionString("Provider=Bogus");
        act.Should().Throw<FormatException>().WithMessage("*Bogus*");
    }

    [Fact]
    public void File_MissingPath_Throws()
    {
        var act = () => CatDb.Database.CatDb.FromConnectionString("Provider=File");
        act.Should().Throw<ArgumentException>().WithMessage("*Path*");
    }

    [Fact]
    public void Network_MissingHost_Throws()
    {
        var act = () => CatDb.Database.CatDb.FromConnectionString("Provider=Network");
        act.Should().Throw<ArgumentException>().WithMessage("*Host*");
    }

    [Fact]
    public void InvalidBoolean_Throws()
    {
        var act = () => CatDb.Database.CatDb.FromConnectionString("Provider=Memory;UseNativeLeafStorage=maybe");
        act.Should().Throw<FormatException>();
    }

    [Fact]
    public void InvalidInteger_Throws()
    {
        var act = () => CatDb.Database.CatDb.FromConnectionString("Provider=Memory;CacheSize=notanumber");
        act.Should().Throw<FormatException>();
    }

    [Fact]
    public void InvalidByteSize_Throws()
    {
        var act = () => CatDb.Database.CatDb.FromConnectionString("Provider=Memory;CacheSizeBytes=lots");
        act.Should().Throw<FormatException>();
    }

    [Fact]
    public void InvalidEnum_Throws()
    {
        var act = () => CatDb.Database.CatDb.FromConnectionString("Provider=Memory;CommitMode=NotAMode");
        act.Should().Throw<FormatException>();
    }
}
