// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.General.Threading;

/// <summary>
/// Fans a batch of independent work items across a bounded number of <b>dedicated</b> threads and
/// returns when all are done. Dedicated threads (not the ThreadPool) are deliberate: this runs while the
/// WTree root lock is held, and ThreadPool threads can all be blocked waiting on that same lock — a
/// <c>Parallel.ForEach</c> there would starve. The calling thread participates, so a batch uses at most
/// <c>degreeOfParallelism</c> threads total and small batches run inline with zero thread overhead.
/// </summary>
internal sealed class ParallelExecutor(int degreeOfParallelism)
{
    private const int MinItemsPerThread = 4;
    private readonly int _dop = Math.Max(1, degreeOfParallelism);

    /// <summary>Runs <paramref name="body"/> over every item, in parallel, blocking until all complete.</summary>
    public void ForEach<T>(IReadOnlyList<T> items, Action<T> body)
    {
        var n = items.Count;
        if (n == 0)
            return;

        var workers = Math.Min(_dop, n / MinItemsPerThread);
        if (workers <= 1)
        {
            for (var i = 0; i < n; i++)
                body(items[i]);
            return;
        }

        var next = -1;
        Exception? failure = null;

        void Work()
        {
            try
            {
                int i;
                while ((i = Interlocked.Increment(ref next)) < n)
                    body(items[i]);
            }
            catch (Exception ex)
            {
                Interlocked.CompareExchange(ref failure, ex, null);
            }
        }

        // Spawn workers-1 helpers; the caller runs the last share itself (so `workers` threads total).
        var helpers = new Thread[workers - 1];
        for (var k = 0; k < helpers.Length; k++)
        {
            helpers[k] = new Thread(Work) { IsBackground = true, Name = "catdb-checkpoint" };
            helpers[k].Start();
        }

        Work();

        foreach (var t in helpers)
            t.Join();

        if (failure is not null)
            throw failure;
    }
}
