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

    private static void FlushUnsafe(string trigger, long now)
    {
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
        Console.Error.WriteLine();
        Console.Error.WriteLine($"[PERFORMANCE_CHECK] grouped metrics | trigger={trigger} | uptimeMs={uptime:F0} | events={eventCount}");
        Console.Error.WriteLine("metric,count,sum,avg,max");

        foreach (var metric in snapshot)
        {
            var count = metric.Value.Count;
            var sum = metric.Value.Sum;
            var avg = count > 0 ? (double)sum / count : 0;
            var max = metric.Value.Max == long.MinValue ? 0 : metric.Value.Max;
            var sumText = Math.Round(sum).ToString(CultureInfo.InvariantCulture);
            var avgText = avg.ToString("F2", CultureInfo.InvariantCulture);
            Console.Error.WriteLine($"{metric.Key},{count},{sumText},{avgText},{max}");
        }

        // Per-window GC deltas — emitted as plain "metric,delta" lines (not aggregates). Compare these against
        // the same window's wtree.commit.hold max: a multi-second hold with a large rt.gc.pause.ms delta means
        // the spike is a GC pause, not checkpoint I/O.
        var gcPauseMs = GC.GetTotalPauseDuration().TotalMilliseconds;
        var gcAllocBytes = GC.GetTotalAllocatedBytes();
        int gen0 = GC.CollectionCount(0), gen1 = GC.CollectionCount(1), gen2 = GC.CollectionCount(2);
        Console.Error.WriteLine($"rt.gc.pause.ms.window,{(gcPauseMs - _lastGcPauseMs):F1}");
        Console.Error.WriteLine($"rt.gc.alloc.mb.window,{(gcAllocBytes - _lastGcAllocBytes) / (1024 * 1024)}");
        Console.Error.WriteLine($"rt.gc.gen0.window,{gen0 - _lastGen0}");
        Console.Error.WriteLine($"rt.gc.gen1.window,{gen1 - _lastGen1}");
        Console.Error.WriteLine($"rt.gc.gen2.window,{gen2 - _lastGen2}");
        _lastGcPauseMs = gcPauseMs;
        _lastGcAllocBytes = gcAllocBytes;
        _lastGen0 = gen0; _lastGen1 = gen1; _lastGen2 = gen2;

        // Process RSS (catches unmanaged/native growth the managed gc.heap.mb misses) + every registered gauge.
        using (var proc = Process.GetCurrentProcess())
            Console.Error.WriteLine($"rt.proc.workingset.mb,{proc.WorkingSet64 / (1024 * 1024)}");

        List<(string Name, Func<long> Sample)> gauges;
        using (Sync.Lock())
            gauges = Gauges.ToList();
        foreach (var (name, sample) in gauges)
        {
            long value;
            try { value = sample(); }
            catch { continue; }   // a dead/disposed structure must never break the flush
            Console.Error.WriteLine($"{name},{value}");
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
