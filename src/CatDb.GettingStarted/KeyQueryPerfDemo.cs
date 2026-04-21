using System.Diagnostics;
using CatDb.Database;
using CatDb.Extensions;
using DatabaseBenchmark;
using CatDb.General.Extensions;

/// <summary>
/// Measures real seek + scan latency on 2M records.
///
/// Performance expectations (local file, SSD):
///   Seek to any key                 : ~microseconds (O(log N))
///   Full range scan 2M records      : depends on IO bandwidth
///   Range scan 10K matching records : ~milliseconds
///   PageAfter (keyset, any depth)   : constant, same as seek
///   Count(full range)               : full scan — O(N)
/// </summary>
static class KeyQueryPerfDemo
{
    private const int TotalRecords = 2_000_000;

    public static void Run(Func<bool, IStorageEngine> openEngine)
    {
        // ── 1. Insert 2M records ──────────────────────────────────────────
        Console.WriteLine($"Inserting {TotalRecords:N0} records...");
        var sw = Stopwatch.StartNew();
        using (var engine = openEngine(true))
        {
            var table = engine.OpenXTable<long, Tick>("perf");
            var c = 0;
            foreach (var kv in TicksGenerator.GetFlow(TotalRecords, KeysType.Random))
            {
                table[kv.Key] = kv.Value;
                if (++c % 500_000 == 0) Console.WriteLine($"  {c:N0}");
            }
            engine.Commit();
        }
        sw.Stop();
        Console.WriteLine($"Insert: {sw.GetSpeed(TotalRecords):N0} rec/sec  ({sw.Elapsed.TotalSeconds:F1}s)");
        Console.WriteLine();

        using var readEngine = openEngine(false);
        var t = readEngine.OpenXTable<long, Tick>("perf");

        // Key range depends on TicksGenerator — keys are longs from the stream.
        // We measure seek-to-first, range scan, and cursor paging.
        var first = t.FirstRow!.Value.Key;
        var last  = t.LastRow!.Value.Key;
        var mid   = first + (last - first) / 2;

        // ── 2. Seek: AtLeast(mid) — should be microseconds ────────────────
        Console.Write($"Seek AtLeast({mid}):          ");
        sw.Restart();
        var seekResult = t.Query(KeyQuery<long>.AtLeast(mid)).FirstOrDefault();
        sw.Stop();
        Console.WriteLine($"{sw.Elapsed.TotalMilliseconds:F3} ms  (key={seekResult.Key})");

        // ── 3. Range count — 5% of range ──────────────────────────────────
        var rangeSize = (last - first) / 20;   // ~5% of key space
        var rangeFrom = mid;
        var rangeTo   = mid + rangeSize;
        Console.Write($"Count Between({rangeFrom},{rangeTo}): ");
        sw.Restart();
        var cnt = t.Count(KeyQuery<long>.Between(rangeFrom, rangeTo));
        sw.Stop();
        Console.WriteLine($"{sw.Elapsed.TotalMilliseconds:F1} ms  ({cnt:N0} records)");

        // ── 4. Range scan — take first 10K from range ─────────────────────
        Console.Write("Scan first 10K from mid:    ");
        sw.Restart();
        var scanned = t.Query(KeyQuery<long>.AtLeast(mid)).Take(10_000).Count();
        sw.Stop();
        Console.WriteLine($"{sw.Elapsed.TotalMilliseconds:F1} ms  ({scanned:N0} records)");

        // ── 5. PageAfter deep — page 10000 (keyset, always O(log N)) ──────
        Console.Write("Cursor page 10000 (keyset): ");
        sw.Restart();
        var isFirstPage = true;
        long cursorKey = 0;
        const int pageSize = 20;
        for (var p = 0; p < 10_000; p++)
        {
            var page = isFirstPage
                ? t.PageAfter(KeyQuery<long>.All(), take: pageSize).ToList()
                : t.PageAfter(KeyQuery<long>.All(), afterKey: cursorKey, take: pageSize).ToList();
            if (page.Count == 0) break;
            cursorKey   = page.Last().Key;
            isFirstPage = false;
        }
        sw.Stop();
        Console.WriteLine($"{sw.Elapsed.TotalMilliseconds:F1} ms total, {sw.Elapsed.TotalMilliseconds / 10_000:F4} ms/page  (cursor={cursorKey})");

        // ── 6. Offset page 10000 via Page() — O(skip), much slower ────────
        Console.Write("Offset Page(skip=200000):   ");
        sw.Restart();
        var offsetPage = t.Page(KeyQuery<long>.All(), skip: 200_000, take: pageSize).ToList();
        sw.Stop();
        var offsetFirst = offsetPage.Count > 0 ? offsetPage[0].Key : 0L;
        Console.WriteLine($"{sw.Elapsed.TotalMilliseconds:F1} ms  (first key={offsetFirst})");

        Console.WriteLine();
        Console.WriteLine("Note: cursor paging is O(log N) per page regardless of depth.");
        Console.WriteLine("      Offset paging is O(skip) — gets slower on deeper pages.");
    }
}
