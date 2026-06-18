// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Globalization;

namespace CatDb.General.Diagnostics;

public static class PerformanceCheck
{
#if PERFORMANCE_CHECK
    private sealed class Aggregate
    {
        public long Count;
        public double Sum;
        public long Max = long.MinValue;

        public void Add(long value)
        {
            Count++;
            Sum += value;
            if (value > Max)
                Max = value;
        }
    }

    private static readonly General.Threading.ReentrantLock Sync = new();
    private static readonly Dictionary<string, Aggregate> Window = new(StringComparer.Ordinal);

    // Gauges = current SIZE of a live structure, sampled once per window flush (not per event). This is the
    // leak-hunting tool: register the size of every suspect collection (cache, heap maps, pending writes, log)
    // and watch which one climbs window-over-window. A growing gauge whose growth tracks a falling throughput
    // is the leak. Samplers run inside the flush (off the hot path) and are wrapped so a throw can't break it.
    private static readonly List<(string Name, Func<long> Sample)> Gauges = new();

    [Conditional("PERFORMANCE_CHECK")]
    public static void RegisterGauge(string name, Func<long> sample)
    {
        using (Sync.Lock())
        {
            // De-dupe by name so re-opening an engine in-process replaces the stale sampler instead of stacking.
            Gauges.RemoveAll(g => string.Equals(g.Name, name, StringComparison.Ordinal));
            Gauges.Add((name, sample));
        }
    }

    private static readonly long StartedAt = Stopwatch.GetTimestamp();
    private static long _lastFlushAt = Stopwatch.GetTimestamp();
    private static long _eventCount;
    private static int _isFlushing;

    private const int FLUSH_INTERVAL_SECONDS = 20;
    private static readonly long FlushIntervalTicks = Stopwatch.Frequency * FLUSH_INTERVAL_SECONDS;

    [Conditional("PERFORMANCE_CHECK")]
    public static void Increment(string key, long delta = 1)
    {
        Observe(key, delta);
    }

    [Conditional("PERFORMANCE_CHECK")]
    public static void Observe(string key, long value)
    {
        using (Sync.Lock())
        {
            if (!Window.TryGetValue(key, out var aggregate))
            {
                aggregate = new Aggregate();
                Window[key] = aggregate;
            }

            aggregate.Add(value);
        }

        Interlocked.Increment(ref _eventCount);
        MaybeFlush("auto");
    }

    [Conditional("PERFORMANCE_CHECK")]
    public static void ObserveDurationTicks(string keyPrefix, long startedAt)
    {
        var elapsedTicks = Stopwatch.GetTimestamp() - startedAt;
        var micros = elapsedTicks * 1_000_000L / Stopwatch.Frequency;
        Observe(keyPrefix + ".us", micros);
    }

    [Conditional("PERFORMANCE_CHECK")]
    public static void MaybeFlush(string trigger)
    {
        var now = Stopwatch.GetTimestamp();
        if (now - Volatile.Read(ref _lastFlushAt) < FlushIntervalTicks)
            return;

        if (Interlocked.Exchange(ref _isFlushing, 1) != 0)
            return;

        try
        {
            now = Stopwatch.GetTimestamp();
            if (now - Volatile.Read(ref _lastFlushAt) < FlushIntervalTicks)
                return;

            FlushUnsafe(trigger, now);
        }
        finally
        {
            Volatile.Write(ref _isFlushing, 0);
        }
    }

    [Conditional("PERFORMANCE_CHECK")]
    public static void Flush(string trigger)
    {
        if (Interlocked.Exchange(ref _isFlushing, 1) != 0)
            return;

        try
        {
            FlushUnsafe(trigger, Stopwatch.GetTimestamp());
        }
        finally
        {
            Volatile.Write(ref _isFlushing, 0);
        }
    }

    // Cumulative GC counters sampled at the previous window flush, so each window can report its DELTA
    // (pause time / collections / allocations that happened during that ~20 s window) — the decisive signal
    // for whether a commit.hold spike is a GC pause vs real checkpoint I/O.
    private static double _lastGcPauseMs;
    private static long _lastGcAllocBytes;
    private static int _lastGen0, _lastGen1, _lastGen2;

    // Crash-survivable log: every flush is also appended (and flushed) to a file, so a hard crash / OOM that
    // kills the process — or even the whole machine — still leaves every window on disk for post-mortem. Path
    // from CATDB_PERFLOG, else ./catdb_perf.log. Opened once, lazily.
    private static StreamWriter? _logWriter;
    private static bool _logInit;

    private static StreamWriter? LogWriter()
    {
        if (_logInit) return _logWriter;
        _logInit = true;
        try
        {
            var path = System.Environment.GetEnvironmentVariable("CATDB_PERFLOG");
            if (string.IsNullOrEmpty(path)) path = "catdb_perf.log";
            // AutoFlush so a crash never loses the last (most interesting) window.
            _logWriter = new System.IO.StreamWriter(path, append: true) { AutoFlush = true };
            _logWriter.WriteLine($"=== PERFORMANCE_CHECK session start {System.DateTime.Now:O} pid={System.Environment.ProcessId} ===");
        }
        catch { _logWriter = null; }
        return _logWriter;
    }

    private static void FlushUnsafe(string trigger, long now)
    {
        var log = LogWriter();
        void Emit(string s) { Console.Error.WriteLine(s); log?.WriteLine(s); }

        List<System.Collections.Generic.KeyValuePair<string, Aggregate>> snapshot;
        long eventCount;

        using (Sync.Lock())
        {
            if (Window.Count == 0)
            {
                _lastFlushAt = now;
                return;
            }

            snapshot = Window.ToList();
            Window.Clear();
            eventCount = Interlocked.Exchange(ref _eventCount, 0);
            _lastFlushAt = now;
        }

        snapshot.Sort((x, y) => string.CompareOrdinal(x.Key, y.Key));

        var uptime = (now - StartedAt) * 1000.0 / Stopwatch.Frequency;
        Emit("");
        Emit($"[PERFORMANCE_CHECK] grouped metrics | trigger={trigger} | uptimeMs={uptime:F0} | events={eventCount}");
        Emit("metric,count,sum,avg,max");

        foreach (var metric in snapshot)
        {
            var count = metric.Value.Count;
            var sum = metric.Value.Sum;
            var avg = count > 0 ? (double)sum / count : 0;
            var max = metric.Value.Max == long.MinValue ? 0 : metric.Value.Max;
            var sumText = Math.Round(sum).ToString(CultureInfo.InvariantCulture);
            var avgText = avg.ToString("F2", CultureInfo.InvariantCulture);
            Emit($"{metric.Key},{count},{sumText},{avgText},{max}");
        }

        // Per-window GC deltas — emitted as plain "metric,delta" lines (not aggregates). Compare these against
        // the same window's wtree.commit.hold max: a multi-second hold with a large rt.gc.pause.ms delta means
        // the spike is a GC pause, not checkpoint I/O.
        var gcPauseMs = GC.GetTotalPauseDuration().TotalMilliseconds;
        var gcAllocBytes = GC.GetTotalAllocatedBytes();
        int gen0 = GC.CollectionCount(0), gen1 = GC.CollectionCount(1), gen2 = GC.CollectionCount(2);
        Emit($"rt.gc.pause.ms.window,{(gcPauseMs - _lastGcPauseMs):F1}");
        Emit($"rt.gc.alloc.mb.window,{(gcAllocBytes - _lastGcAllocBytes) / (1024 * 1024)}");
        Emit($"rt.gc.gen0.window,{gen0 - _lastGen0}");
        Emit($"rt.gc.gen1.window,{gen1 - _lastGen1}");
        Emit($"rt.gc.gen2.window,{gen2 - _lastGen2}");
        _lastGcPauseMs = gcPauseMs;
        _lastGcAllocBytes = gcAllocBytes;
        _lastGen0 = gen0; _lastGen1 = gen1; _lastGen2 = gen2;

        // ── Absolute memory snapshot (these are the LEAK detectors — watch any one climb every window) ──
        // Split managed vs native so we know WHICH side leaks:
        //   rt.gc.* = managed heap detail (incl. LOH/POH — large byte[]/pinned buffers, a classic node-store leak)
        //   rt.proc.privatebytes = total committed incl. UNMANAGED (a native leak grows here but not in gc.heap)
        var mi = GC.GetGCMemoryInfo();
        Emit($"rt.gc.heapsize.mb,{mi.HeapSizeBytes / (1024 * 1024)}");
        Emit($"rt.gc.committed.mb,{mi.TotalCommittedBytes / (1024 * 1024)}");
        Emit($"rt.gc.fragmented.mb,{mi.FragmentedBytes / (1024 * 1024)}");
        var gi = mi.GenerationInfo;                       // [0]=gen0 [1]=gen1 [2]=gen2 [3]=LOH [4]=POH
        if (gi.Length > 2) Emit($"rt.gc.gen2.size.mb,{gi[2].SizeAfterBytes / (1024 * 1024)}");
        if (gi.Length > 3) Emit($"rt.gc.loh.size.mb,{gi[3].SizeAfterBytes / (1024 * 1024)}");
        if (gi.Length > 4) Emit($"rt.gc.poh.size.mb,{gi[4].SizeAfterBytes / (1024 * 1024)}");
        Emit($"rt.gc.pinned.count,{mi.PinnedObjectsCount}");

        using (var proc = Process.GetCurrentProcess())
        {
            Emit($"rt.proc.workingset.mb,{proc.WorkingSet64 / (1024 * 1024)}");      // RSS (resident)
            Emit($"rt.proc.privatebytes.mb,{proc.PrivateMemorySize64 / (1024 * 1024)}"); // committed incl native
        }

        List<(string Name, Func<long> Sample)> gauges;
        using (Sync.Lock())
            gauges = Gauges.ToList();
        foreach (var (name, sample) in gauges)
        {
            long value;
            try { value = sample(); }
            catch { continue; }   // a dead/disposed structure must never break the flush
            Emit($"{name},{value}");
        }
    }
#else
    [Conditional("PERFORMANCE_CHECK")]
    public static void RegisterGauge(string name, Func<long> sample)
    {
    }

    [Conditional("PERFORMANCE_CHECK")]
    public static void Increment(string key, long delta = 1)
    {
    }

    [Conditional("PERFORMANCE_CHECK")]
    public static void Observe(string key, long value)
    {
    }

    [Conditional("PERFORMANCE_CHECK")]
    public static void ObserveDurationTicks(string keyPrefix, long startedAt)
    {
    }

    [Conditional("PERFORMANCE_CHECK")]
    public static void MaybeFlush(string trigger)
    {
    }

    [Conditional("PERFORMANCE_CHECK")]
    public static void Flush(string trigger)
    {
    }
#endif
}
