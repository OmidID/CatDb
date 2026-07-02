// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections.Concurrent;

namespace CatDb.General.Collections;

/// <summary>
/// Grace-period reclaim for <see cref="NativeOrderedSet"/>s the engine drops (leaf eviction, merged-away
/// sources, emptied containers). Immediate Dispose would race a page-scan reader that still holds the set
/// reference for a moment after the branch lock is released; relying on finalizers lets native memory grow
/// unbounded under load (finalization lags eviction — measured 2.3&#160;GB in 5&#160;min). Instead a dropped
/// set is queued here and disposed once it is at least <see cref="GraceMs"/> old — far beyond the
/// microsecond window a reader needs to enter its read lock (Dispose additionally takes the set's write
/// lock, so an in-flight reader always finishes first).
/// </summary>
public static class NativeReclaim
{
    private const int GraceMs = 30_000;

    private static readonly ConcurrentQueue<(long DropTicks, NativeOrderedSet Set)> Queue = new();
    private static long _queuedCount;

    /// <summary>Total sets currently awaiting reclaim (diagnostics).</summary>
    public static long PendingCount => Interlocked.Read(ref _queuedCount);

    /// <summary>Queues a dropped set for deferred disposal. Safe for non-native/null sets (no-op).</summary>
    public static void Defer(IOrderedSet<IData, IData>? set)
    {
        if (set is not NativeOrderedSet native)
            return;
        Queue.Enqueue((System.Environment.TickCount64, native));
        Interlocked.Increment(ref _queuedCount);
    }

    /// <summary>Disposes every queued set older than the grace period. Called from the background
    /// checkpoint worker (~2 s cadence) — never on a hot path.</summary>
    public static void Drain()
    {
        var now = System.Environment.TickCount64;
        while (Queue.TryPeek(out var entry) && now - entry.DropTicks >= GraceMs)
        {
            if (!Queue.TryDequeue(out entry))
                break;
            Interlocked.Decrement(ref _queuedCount);
            entry.Set.Dispose();
        }
    }

    /// <summary>Disposes everything regardless of age — engine shutdown only.</summary>
    public static void DrainAll()
    {
        while (Queue.TryDequeue(out var entry))
        {
            Interlocked.Decrement(ref _queuedCount);
            entry.Set.Dispose();
        }
    }
}
