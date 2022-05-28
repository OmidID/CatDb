using CatDb.General.Communication;
using CatDb.General.IO;
using CatDb.Remote;
using CatDb.Storage;
using CatDb.WaterfallTree;

namespace CatDb.Database
{
    public static class CatDb
    {
        public static IStorageEngine FromHeap(IHeap heap)
        {
            return new StorageEngine(heap);
        }

        public static IStorageEngine FromStream(Stream stream)
        {
            IHeap heap = new Heap(stream, false, AllocationStrategy.FromTheCurrentBlock);

            return FromHeap(heap);
        }

        public static IStorageEngine FromMemory()
        {
            var stream = new MemoryStream();

            return FromStream(stream);
        }

        public static IStorageEngine FromFile(string fileName)
        {
            var stream = new OptimizedFileStream(fileName, FileMode.OpenOrCreate);

            return FromStream(stream);
        }

        public static IStorageEngine FromNetwork(string host, int port = 7182)
        {
            return new StorageEngineClient(host, port);
        }

        public static StorageEngineServer CreateServer(IStorageEngine engine, int port = 7182)
        {
            var server = new TcpServer(port);
            var engineServer = new StorageEngineServer(engine, server);

            return engineServer;
        }
    }
}