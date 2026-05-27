using CatDb.General.Communication;
using CatDb.General.IO;
using CatDb.Remote;
using CatDb.Storage;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public static class CatDb
{
    public static IStorageEngine FromHeap(IHeap heap, DatabaseOptions? options = null) =>
        new StorageEngine(heap, options);

    public static IStorageEngine FromStream(Stream stream, DatabaseOptions? options = null) =>
        FromHeap(new Heap(stream), options);

    public static IStorageEngine FromMemory(DatabaseOptions? options = null) =>
        FromStream(new MemoryStream(), options);

    /// <summary>
    /// Open or create a database from a file.
    /// Default commit mode is WriteAheadLog (crash-safe).
    /// </summary>
    public static IStorageEngine FromFile(string fileName, DatabaseOptions? options = null)
    {
        options ??= DatabaseOptions.Default;
        var stream = new OptimizedFileStream(fileName, FileMode.OpenOrCreate);
        var heap = new Heap(stream);

        if (options.CommitMode == CommitMode.WriteAheadLog)
        {
            var walPath = fileName + ".wal";
            var walHeap = new WalHeap(heap, walPath);
            return new StorageEngine(walHeap, options);
        }

        return new StorageEngine(heap, options);
    }

    public static IStorageEngine FromNetwork(
        string host,
        int port = 7182,
        string databaseName = "default",
        string? userName = null,
        string? password = null) =>
        new StorageEngineClient(host, port, databaseName, userName, password);

    /// <summary>Fully async version of <see cref="FromNetwork"/>.</summary>
    public static async Task<IStorageEngine> FromNetworkAsync(
        string host,
        int port = 7182,
        string databaseName = "default",
        string? userName = null,
        string? password = null,
        CancellationToken ct = default)
    {
        var client = StorageEngineClient.CreateUnconnected(host, port, databaseName, userName, password);
        await client.ConnectAsync(ct).ConfigureAwait(false);
        return client;
    }

    public static StorageEngineServer CreateServer(IStorageEngine engine, int port = 7182)
    {
        var server       = new TcpServer(port);
        var engineServer = new StorageEngineServer(engine, server);
        return engineServer;
    }
}