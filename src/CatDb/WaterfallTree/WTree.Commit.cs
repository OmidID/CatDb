// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections.Concurrent;
using CatDb.Database;
using CatDb.General.Threading;
using CatDb.Storage;

namespace CatDb.WaterfallTree;

public partial class WTree
{
    /// <summary>
    /// Strategy for persisting dirty nodes at commit time, chosen from
    /// <see cref="DatabaseOptions.CommitDurability"/> (see <see cref="CreateCommitStrategy"/>). Nested because
    /// <see cref="Node"/> is private. The default <see cref="SynchronousCommitStrategy"/> is exactly the
    /// historical inline-store behaviour.
    /// </summary>
    private interface ICommitStrategy : IDisposable
    {
        /// <summary>Called before the commit takes the root lock — lets a deferred strategy wait for its
        /// previous background checkpoint so this commit never mutates a node still being serialised.</summary>
        void BeginCommit();

        /// <summary>Persist a dirty node found during a commit Fall (called under the root lock).</summary>
        void Persist(Node node);

        /// <summary>Serialise the collected dirty nodes into the heap's write buffer. Runs UNDER the root lock
        /// (node serialisation must see a quiescent tree — a node can be merged/split only under that lock).</summary>
        void StorePending();

        /// <summary>Durably harden the heap (WAL burst + fsync). Safe to run OUTSIDE the root lock —
        /// <see cref="Storage.WalHeap.Commit"/> snapshots the pending writes under its own commit lock and never
        /// blocks readers/writers — so the fsync no longer freezes the world during a checkpoint.</summary>
        void HardenHeap(IHeap heap);
    }

    /// <summary>Inline persistence — serialise+write each node now (under the root lock); fsync separately.</summary>
    private sealed class SynchronousCommitStrategy : ICommitStrategy
    {
        public void BeginCommit() { }
        public void Persist(Node node) => node.Store();
        public void StorePending() { }                       // already stored inline in Persist
        public void HardenHeap(IHeap heap) => heap.Commit();
        public void Dispose() { }
    }

    /// <summary>
    /// Dirty nodes are serialised+written in parallel across dedicated threads (<see cref="ParallelExecutor"/>),
    /// shrinking the root-lock hold. <c>_pending</c> needs no lock — Persist+StorePending run single-threaded
    /// inside the one checkpoint holding the root lock; the fsync (HardenHeap) runs after the lock is released.
    /// </summary>
    private sealed class ParallelCheckpointCommitStrategy(int degreeOfParallelism) : ICommitStrategy
    {
        private readonly List<Node> _pending = [];
        private readonly ParallelExecutor _executor = new(degreeOfParallelism);

        public void BeginCommit() { }
        public void Persist(Node node) => _pending.Add(node);

        public void StorePending()
        {
            if (_pending.Count > 0)
            {
                _executor.ForEach(_pending, static n => n.Store());
                _pending.Clear();
            }
        }

        public void HardenHeap(IHeap heap) => heap.Commit();

        public void Dispose() { }
    }

    /// <summary>
    /// Delayed durability: the commit collects its dirty nodes and returns immediately; a background worker
    /// re-acquires the root lock to serialise (in parallel) and fsync them. The next commit's
    /// <see cref="BeginCommit"/> waits for that worker, so a node is never mutated while it is being stored —
    /// no per-node locking, no races. The window between a commit returning and its background fsync is the
    /// delayed-durability tradeoff; committed data older than the last finished checkpoint is always durable.
    /// Requires <see cref="CommitMode.WriteAheadLog"/>.
    /// </summary>
    private sealed class AsyncDeferredCommitStrategy : ICommitStrategy
    {
        private readonly WTree _tree;
        private readonly ParallelExecutor _executor;
        private readonly List<Node> _pending = [];
        private readonly BlockingCollection<(Node[] Nodes, IHeap Heap)> _queue = new(new ConcurrentQueue<(Node[], IHeap)>());
        private readonly ManualResetEventSlim _idle = new(true); // set ⇔ no checkpoint in flight
        private readonly Thread _worker;
        private volatile Exception? _failure;

        public AsyncDeferredCommitStrategy(WTree tree, int degreeOfParallelism)
        {
            _tree = tree;
            _executor = new ParallelExecutor(degreeOfParallelism);
            _worker = new Thread(WorkerLoop) { IsBackground = true, Name = "catdb-async-checkpoint" };
            _worker.Start();
        }

        public void BeginCommit()
        {
            _idle.Wait(); // previous checkpoint must finish before this commit's Fall can re-touch those nodes
            Rethrow();
        }

        public void Persist(Node node) => _pending.Add(node);

        public void StorePending() { }   // deferred — the worker stores (under its own root-lock re-acquire)

        public void HardenHeap(IHeap heap)
        {
            if (_pending.Count == 0)
            {
                heap.Commit();
                return;
            }

            var batch = _pending.ToArray();
            _pending.Clear();
            _idle.Reset();
            _queue.Add((batch, heap)); // worker stores + fsyncs outside the committing thread
        }

        private void WorkerLoop()
        {
            foreach (var (nodes, heap) in _queue.GetConsumingEnumerable())
            {
                // Re-acquire the root lock so the store sees a quiescent tree (same guarantee as an inline
                // commit) — the committing thread already released it, so reads/writes ran in between.
                _tree._rootBranch.SyncRoot.Enter();
                try
                {
                    _executor.ForEach(nodes, static n => n.Store());
                    heap.Commit();
                }
                catch (Exception ex)
                {
                    _failure = ex;
                }
                finally
                {
                    _tree._rootBranch.SyncRoot.Exit();
                    _idle.Set();
                }
            }
        }

        private void Rethrow()
        {
            var ex = _failure;
            if (ex is null) return;
            _failure = null;
            throw new InvalidOperationException("Background checkpoint failed.", ex);
        }

        public void Dispose()
        {
            _idle.Wait();             // flush the last in-flight checkpoint
            _queue.CompleteAdding();
            _worker.Join();
            Rethrow();
            _idle.Dispose();
            _queue.Dispose();
        }
    }

    private ICommitStrategy CreateCommitStrategy(DatabaseOptions? options)
    {
        // TransactionLog already removes per-commit node serialisation (commit = log fsync); its checkpoint
        // is occasional, so a simple synchronous store keeps the truncate-after-heap-commit ordering exact.
        if (options?.CommitMode == CommitMode.TransactionLog)
            return new ParallelCheckpointCommitStrategy(Environment.ProcessorCount);

        return options?.CommitDurability switch
        {
            CommitDurability.ParallelCheckpoint => new ParallelCheckpointCommitStrategy(Environment.ProcessorCount),
            CommitDurability.AsyncDeferred => new AsyncDeferredCommitStrategy(this, Environment.ProcessorCount),
            _ => new SynchronousCommitStrategy(),
        };
    }
}
