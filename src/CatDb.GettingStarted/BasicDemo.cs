using System.Diagnostics;
using CatDb.Database;
using CatDb.General.Extensions;
using DatabaseBenchmark;

/// <summary>
/// Original insert + read benchmark — 1M records.
/// </summary>
static class BasicDemo
{
    public static void Run(Func<bool, IStorageEngine> openEngine)
    {
        const int    tickCount = 1_000_000;
        const KeysType keysType = KeysType.Random;
        var sw = new Stopwatch();

        Console.WriteLine("Inserting...");
        sw.Restart();
        var c = 0;
        using (var engine = openEngine(true))
        {
            var table = engine.OpenXTable<long, Tick>("table");
            foreach (var kv in TicksGenerator.GetFlow(tickCount, keysType))
            {
                table[kv.Key] = kv.Value;
                if (++c % 100_000 == 0) Console.WriteLine($"  Inserted {c:N0}");
            }

            var table2 = engine.OpenXTable<string, string>("table2");
            table2["My Random Key"]  = "Random Value";
            table2["My Random Key2"] = "Random Value2";
            engine.Commit();
        }
        sw.Stop();
        Console.WriteLine($"Insert speed : {sw.GetSpeed(tickCount):N0} rec/sec");

        Console.WriteLine();
        Console.WriteLine("Reading...");
        sw.Restart();
        c = 0;
        using (var engine = openEngine(false))
        {
            var table = engine.OpenXTable<long, Tick>("table");
            foreach (var _ in table)
            {
                if (++c % 100_000 == 0) Console.WriteLine($"  Read {c:N0}");
            }

            var table2 = engine.OpenXTable<string, string>("table2");
            Console.WriteLine($"  table2[\"My Random Key\"]  = {table2["My Random Key"]}");
            Console.WriteLine($"  table2[\"My Random Key2\"] = {table2["My Random Key2"]}");
        }
        sw.Stop();
        Console.WriteLine($"Read speed   : {sw.GetSpeed(c):N0} rec/sec");
    }
}
