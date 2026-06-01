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
    }
#else
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
