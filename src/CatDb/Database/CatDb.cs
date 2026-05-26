using CatDb.General.Communication;
using CatDb.General.IO;
using CatDb.Remote;
using CatDb.Storage;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public static class CatDb
{
    public static IStorageEngine FromHeap(IHeap heap) =>
        new StorageEngine(heap);

    public static IStorageEngine FromStream(Stream stream, CommitMode commitMode = CommitMode.InPlace) =>
        FromHeap(new Heap(stream));

    public static IStorageEngine FromMemory() =>
        FromStream(new MemoryStream(), CommitMode.InPlace);

    /// <summary>
    /// Open or create a database from a file.
    /// Default commit mode is WriteAheadLog (crash-safe).
    /// </summary>
    public static IStorageEngine FromFile(string fileName, CommitMode commitMode = CommitMode.WriteAheadLog)
    {
        var stream = new OptimizedFileStream(fileName, FileMode.OpenOrCreate);
        var heap = new Heap(stream);

        if (commitMode == CommitMode.WriteAheadLog)
        {
            var walPath = fileName + ".wal";
            var walHeap = new WalHeap(heap, walPath);
            return new StorageEngine(walHeap);
        }

        return new StorageEngine(heap);
    }

    public static IStorageEngine FromNetwork(string host, int port = 7182) =>
        new StorageEngineClient(host, port);

    /// <summary>Fully async version of <see cref="FromNetwork"/>.</summary>
    public static async Task<IStorageEngine> FromNetworkAsync(
        string host, int port = 7182, CancellationToken ct = default)
    {
        var client = StorageEngineClient.CreateUnconnected(host, port);
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