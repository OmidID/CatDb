// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

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
        Console.Write($"Count (range ~5%):          ");
        sw.Restart();
        var cnt = t.Count(KeyQuery<long>.Between(rangeFrom, rangeTo));
        sw.Stop();
        Console.WriteLine($"{sw.Elapsed.TotalMilliseconds:F3} ms  ({cnt:N0} records)");

        Console.Write("Count (full 2M):            ");
        sw.Restart();
        var cntAll = t.Count(KeyQuery<long>.All());
        sw.Stop();
        Console.WriteLine($"{sw.Elapsed.TotalMilliseconds:F3} ms  ({cntAll:N0} records)");

        // ── 4. Range scan — take first 10K from range ─────────────────────
        Console.Write("Scan first 10K from mid:    ");
        sw.Restart();
        var scanned = t.Query(KeyQuery<long>.AtLeast(mid)).Take(10_000).Count();
        sw.Stop();
        Console.WriteLine($"{sw.Elapsed.TotalMilliseconds:F1} ms  ({scanned:N0} records)");

        // ── 4b. Compare: Query.Scan engine path vs raw Forward() ──────────
        Console.WriteLine();
        Console.WriteLine("── Engine Scan vs raw Forward() comparison ──");

        // Raw Forward() — the old approach (3 yield layers, lock held during iteration)
        Console.Write("Forward() scan 100K:        ");
        sw.Restart();
        var fwdCount = 0;
        foreach (var kv in t.Forward(mid, true, default!, false))
        {
            fwdCount++;
            if (fwdCount >= 100_000) break;
        }
        sw.Stop();
        var fwdMs = sw.Elapsed.TotalMilliseconds;
        Console.WriteLine($"{fwdMs:F1} ms  ({fwdCount:N0} records, {fwdCount / fwdMs * 1000:N0} rec/sec)");

        // Engine Scan (segment-based, 1 yield layer, buffer per leaf)
        Console.Write("Query.Scan 100K:            ");
        sw.Restart();
        var scanCount = t.Query(KeyQuery<long>.AtLeast(mid)).Take(100_000).Count();
        sw.Stop();
        var scanMs = sw.Elapsed.TotalMilliseconds;
        Console.WriteLine($"{scanMs:F1} ms  ({scanCount:N0} records, {scanCount / scanMs * 1000:N0} rec/sec)");

        if (fwdMs > 0)
            Console.WriteLine($"Speedup: {fwdMs / scanMs:F1}x");

        // Full table scan comparison
        Console.Write("Forward() full scan:        ");
        sw.Restart();
        var fullFwd = 0;
        foreach (var kv in t.Forward()) fullFwd++;
        sw.Stop();
        var fullFwdMs = sw.Elapsed.TotalMilliseconds;
        Console.WriteLine($"{fullFwdMs:F1} ms  ({fullFwd:N0} records, {fullFwd / fullFwdMs * 1000:N0} rec/sec)");

        Console.Write("Query.Scan full scan:       ");
        sw.Restart();
        var fullScan = t.Count(KeyQuery<long>.All());
        sw.Stop();
        var fullScanMs = sw.Elapsed.TotalMilliseconds;
        Console.WriteLine($"{fullScanMs:F1} ms  ({fullScan:N0} records, {fullScan / fullScanMs * 1000:N0} rec/sec)");

        if (fullFwdMs > 0)
            Console.WriteLine($"Speedup: {fullFwdMs / fullScanMs:F1}x");
        Console.WriteLine();
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
