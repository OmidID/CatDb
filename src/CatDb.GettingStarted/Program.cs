using System.Diagnostics;
using CatDb.Data;
using CatDb.General.Extensions;
using DatabaseBenchmark;
using CatDb.Database;
using Database = CatDb.Database;

// ── Switch between local file and remote server ───────────────────────────────
const bool   USE_SERVER  = true;          // true = connect to CatDb.Server
const string SERVER_HOST = "localhost";
const int    SERVER_PORT = 7182;
const string FILE_NAME   = "test.CatDb";
// ─────────────────────────────────────────────────────────────────────────────

Example(1_000_000, KeysType.Random);
Console.ReadKey();

static IStorageEngine OpenEngine()
{
    if (USE_SERVER)
    {
        Console.WriteLine($"Connecting to server {SERVER_HOST}:{SERVER_PORT}...");
        return Database.CatDb.FromNetwork(SERVER_HOST, SERVER_PORT);
    }

    File.Delete(FILE_NAME);
    return Database.CatDb.FromFile(FILE_NAME);
}

static void Example(int tickCount, KeysType keysType)
{
    var sw = new Stopwatch();

    // insert
    Console.WriteLine("Inserting...");
    sw.Restart();
    var c = 0;
    using (var engine = OpenEngine())
    {
        var table = engine.OpenXTable<long, Tick>("table");

        foreach (var kv in TicksGenerator.GetFlow(tickCount, keysType))
        {
            table[kv.Key] = kv.Value;
            if (++c % 100_000 == 0) Console.WriteLine("Inserted {0} records", c);
        }

        var table2 = engine.OpenXTable<string, string>("table2");
        table2["My Random Key"]  = "Random Value";
        table2["My Random Key2"] = "Random Value2";

        engine.Commit();
    }
    sw.Stop();
    Console.WriteLine("Insert speed: {0} rec/sec", sw.GetSpeed(tickCount));

    // read
    Console.WriteLine("Reading...");
    sw.Restart();
    c = 0;
    using (var engine = OpenEngine())
    {
        var table = engine.OpenXTable<long, Tick>("table");

        foreach (var _ in table)
        {
            if (++c % 100_000 == 0) Console.WriteLine("Read {0} records", c);
        }

        var table2 = engine.OpenXTable<string, string>("table2");
        Console.WriteLine(table2["My Random Key"]);
        Console.WriteLine(table2["My Random Key2"]);
    }
    sw.Stop();
    Console.WriteLine("Read speed: {0} rec/sec", sw.GetSpeed(c));
}
