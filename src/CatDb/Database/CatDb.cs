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

    public static IStorageEngine FromStream(Stream stream) =>
        FromHeap(new Heap(stream));

    public static IStorageEngine FromMemory() =>
        FromStream(new MemoryStream());

    public static IStorageEngine FromFile(string fileName) =>
        FromStream(new OptimizedFileStream(fileName, FileMode.OpenOrCreate));

    public static IStorageEngine FromNetwork(string host, int port = 7182) =>
        new StorageEngineClient(host, port);

    public static StorageEngineServer CreateServer(IStorageEngine engine, int port = 7182)
    {
        var server       = new TcpServer(port);
        var engineServer = new StorageEngineServer(engine, server);
        return engineServer;
    }
}