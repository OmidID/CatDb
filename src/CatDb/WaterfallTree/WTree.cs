// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
using System.Collections.Concurrent;
using System.Diagnostics;
using CatDb.Data;
using CatDb.Database;
using CatDb.General.Collections;
using CatDb.General.Threading;

namespace CatDb.WaterfallTree;
public partial class WTree : IDisposable
{
    private int _internalNodeMinBranches = 2; //default values
    private int _internalNodeMaxBranches = 64;
    private long _cacheSizeBytes = 0;
    private readonly ICommitStrategy _commitStrategy;

    // ── TransactionLog mode (CommitMode.TransactionLog) ───────────────────────
    // Logical redo log: commit appends ops + fsyncs (cheap); dirty nodes flush at an occasional
    // background/inline checkpoint, which then truncates the log. Null in InPlace/WriteAheadLog modes.
    private readonly Storage.OperationLog? _operationLog;
    private long _lsn;            // monotonic op LSN (incremented under the root lock in Execute)
    private long _checkpointLsn;  // last checkpointed LSN — persisted in Settings v2, recovery boundary
    private bool _replaying;      // true while replaying the log on open (suppresses re-append)
    private int _checkpointIntervalMs = 10_000;
    private long _checkpointLogSizeBytes = 64L * 1024 * 1024;
    private long _lastCheckpointTicks;
    private bool _incrementalCheckpoint;
    private int _checkpointMaxNodes = 64;
    private bool _incrementalActiveCheckpoint;  // true only inside an incremental CheckpointToHeap pass
    private int _internalNodeMaxOperationsInRoot = 4 * 1024;
    private int _internalNodeMinOperations = 4 * 1024;
    private int _internalNodeMaxOperations = 8 * 1024;
    private int _leafNodeMinRecords = 4 * 1024;
    private int _leafNodeMaxRecords = 8 * 1024;

    //reserved handles
    private const long HANDLE_SETTINGS = 0;
    private const long HANDLE_SCHEME = 1;
    private const long HANDLE_ROOT = 2;
    private const long HANDLE_RESERVED = 3;

    private readonly Countdown _workingFallCount = new();
    private readonly Branch _rootBranch;
    private bool _isRootCacheLoaded;

    private volatile bool _disposed;
    public bool IsDisposed => _disposed;
    private int _depth = 1;

    private long _globalVersion;

    public long GlobalVersion
    {
        get => Interlocked.Read(ref _globalVersion);
        set => Interlocked.Exchange(ref _globalVersion, value);
    }

    private readonly Scheme _scheme;
    private readonly IHeap _heap;

    public WTree(IHeap heap, DatabaseOptions? options = null, Storage.OperationLog? operationLog = null)
    {
        if (heap == null)
            throw new NullReferenceException("heap");

        _heap = heap;
        _operationLog = operationLog;
        _commitStrategy = CreateCommitStrategy(options);

#if PERFORMANCE_CHECK
        // Leak-hunting gauges — current size of every structure that could grow without bound. Watch which
        // climbs window-over-window while throughput falls. Sampled once per flush, off the hot path.
        General.Diagnostics.PerformanceCheck.RegisterGauge("gauge.wtree.cachesizebytes.mb", () => _cacheSizeBytes / (1024 * 1024));
        General.Diagnostics.PerformanceCheck.RegisterGauge("gauge.wtree.cache.count", () => _cache.Count);
        General.Diagnostics.PerformanceCheck.RegisterGauge("gauge.wtree.cache.bytes.mb", () =>
        {
            long total = 0;
            foreach (var n in _cache.Values) total += n.ApproxByteSize;
            return total / (1024 * 1024);
        });
        if (_operationLog != null)
            General.Diagnostics.PerformanceCheck.RegisterGauge("gauge.oplog.size.mb", () => _operationLog.SizeBytes / (1024 * 1024));
#endif

        if (options != null)
        {
            _checkpointIntervalMs = options.CheckpointIntervalMs;
            _checkpointLogSizeBytes = options.CheckpointLogSizeBytes;
            _incrementalCheckpoint = options.IncrementalCheckpoint;
            _checkpointMaxNodes = options.CheckpointMaxNodes;
        }

        // Apply options for NEW databases (existing DBs load settings from header)
        if (options != null && !heap.Exists(HANDLE_SETTINGS))
        {
            _internalNodeMaxBranches = options.MaxBranchesPerNode;
            _internalNodeMinBranches = Math.Max(2, options.MaxBranchesPerNode / 32);
            _internalNodeMaxOperationsInRoot = options.MaxOperationsInRoot;
            _internalNodeMinOperations = options.MinOperationsPerNode;
            _internalNodeMaxOperations = options.MaxOperationsPerNode;
            _leafNodeMinRecords = options.MinRecordsPerLeaf;
            _leafNodeMaxRecords = options.MaxRecordsPerLeaf;
            _cacheSize = options.CacheSize;
        }

        // Runtime memory knob (not persisted in the DB header) — apply for new and existing DBs.
        if (options != null)
            _cacheSizeBytes = options.CacheSizeBytes;

        var existingDb = heap.Exists(HANDLE_SETTINGS);
        if (existingDb)
        {
            //create root branch with dummy handle
            _rootBranch = new Branch(this, NodeType.Leaf, 0);

            //read settings - settings will set the RootBranch.NodeHandle
            using (var ms = new MemoryStream(heap.Read(HANDLE_SETTINGS)))
                Settings.Deserialize(this, ms);

            //read scheme
            using (var ms = new MemoryStream(heap.Read(HANDLE_SCHEME)))
                _scheme = Scheme.Deserialize(new BinaryReader(ms));

            ////load branch cache
            //using (MemoryStream ms = new MemoryStream(Heap.Read(HANDLE_ROOT)))
            //    RootBranch.Cache.Load(this, new BinaryReader(ms));
            _isRootCacheLoaded = false;
        }
        else
        {
            //obtain reserved handles
            var handle = heap.ObtainNewHandle();
            if (handle != HANDLE_SETTINGS)
                throw new Exception("Logical error.");

            _scheme = new Scheme();
            handle = heap.ObtainNewHandle();
            if (handle != HANDLE_SCHEME)
                throw new Exception("Logical error.");

            handle = heap.ObtainNewHandle();
            if (handle != HANDLE_ROOT)
                throw new Exception("Logical error.");

            handle = heap.ObtainNewHandle();
            if (handle != HANDLE_RESERVED)
                throw new Exception("Logical error.");

            _rootBranch = new Branch(this, NodeType.Leaf); //the constructor will invoke Heap.ObtainHandle()

            _isRootCacheLoaded = true;
        }

        _lastCheckpointTicks = Environment.TickCount64;

        if (_operationLog != null)
        {
            if (existingDb)
                // Heap reflects state ≤ _checkpointLsn; replay the log tail (> that).
                RecoverFromLog();
            else
                // New DB: write an initial (empty) checkpoint so the heap structure (settings/scheme/root)
                // exists for reopen — subsequent commits are cheap log fsyncs.
                CheckpointToHeap(CancellationToken.None);
        }
    }

    /// <summary>Replays committed op-log records with LSN &gt; the persisted checkpoint LSN, re-applying
    /// them in memory (without re-logging) so the tree resumes the last committed state.</summary>
    private void RecoverFromLog()
    {
        _replaying = true;
        try
        {
            if (!_isRootCacheLoaded)
                LoadRootCache();

            _lsn = _checkpointLsn;
            _operationLog!.Recover(_checkpointLsn, (lsn, reader) =>
            {
                ReplayLogRecord(lsn, reader);
                _lsn = lsn;
            });
        }
        finally
        {
            _replaying = false;
        }
    }

    private void LoadRootCache()
    {
        using (var ms = new MemoryStream(_heap.Read(HANDLE_ROOT)))
            _rootBranch.Cache.Load(this, new BinaryReader(ms));

        _isRootCacheLoaded = true;
    }

    private void Sink()
    {
        if (_rootBranch.NodeState != NodeState.None)
        {
            var token = new Token(CancellationToken.None);
            _rootBranch.MaintenanceRoot(token);
            _rootBranch.Node.Touch(_depth + 1);
        }

        _rootBranch.Fall(_depth + 1, new Token(CancellationToken.None), new Params(WalkMethod.Current, WalkAction.None, null, true));
    }

    public void Execute(IOperationCollection operations)
    {
        if (_disposed)
            throw new ObjectDisposedException("WTree");

        _rootBranch.SyncRoot.Enter();
#if PERFORMANCE_CHECK
        var holdStart = System.Diagnostics.Stopwatch.GetTimestamp();
#endif
        try
        {
            if (!_isRootCacheLoaded)
                LoadRootCache();

            // Stamp + log BEFORE applying to the cache so the buffered ops already carry their LSN
            // (the incremental checkpoint reads op LSNs out of the cache/leaves).
            if (_operationLog != null && !_replaying)
            {
                var lsn = ++_lsn;
                StampLsn(operations, lsn);
                AppendToLog(operations.Locator, operations, lsn);
            }

            _rootBranch.ApplyToCache(operations);

            if (_rootBranch.Cache.OperationCount > _internalNodeMaxOperationsInRoot)
                Sink();
        }
        finally
        {
#if PERFORMANCE_CHECK
            General.Diagnostics.PerformanceCheck.ObserveDurationTicks("wtree.execute.hold", holdStart);
#endif
            _rootBranch.SyncRoot.Exit();
        }
    }

    public void Execute(Locator locator, IOperation operation)
    {
        if (_disposed)
            throw new ObjectDisposedException("WTree");

        _rootBranch.SyncRoot.Enter();
        try
        {
            if (!_isRootCacheLoaded)
                LoadRootCache();

            if (_operationLog != null && !_replaying)
            {
                var lsn = ++_lsn;
                operation.Lsn = lsn;
                var single = locator.OperationCollectionFactory.Create(1);
                single.Add(operation);
                AppendToLog(locator, single, lsn);
            }

            _rootBranch.ApplyToCache(locator, operation);

            if (_rootBranch.Cache.OperationCount > _internalNodeMaxOperationsInRoot)
                Sink();
        }
        finally
        {
            _rootBranch.SyncRoot.Exit();
        }
    }

    // ── TransactionLog: append / replay / checkpoint ──────────────────────────

    private readonly HashSet<long> _loggedLocators = []; // locator ids persisted to the heap scheme

    /// <summary>Appends one applied op-batch to the redo log (under the root lock; LSN = apply order).</summary>
    private void AppendToLog(Locator locator, IOperationCollection operations, long lsn)
    {
        // Ensure the locator's schema is durable in the heap before its ops are logged, so replay can
        // resolve the id even after a crash before the next checkpoint. Rare (once per table/index).
        if (_loggedLocators.Add(locator.Id))
            PersistScheme();

        _operationLog!.Append(lsn, w =>
        {
            SerializeLocator(w, locator);
            locator.OperationsPersist.Write(w, operations);
        });
    }

    /// <summary>Stamps every op in a batch with its append LSN (transient; for incremental-checkpoint tracking).</summary>
    private static void StampLsn(IOperationCollection operations, long lsn)
    {
        for (var i = 0; i < operations.Count; i++)
            operations[i].Lsn = lsn;
    }

    /// <summary>Decodes and re-applies one log record during recovery (no re-logging — <c>_replaying</c>).
    /// Stamps the batch's LSN onto the ops so leaves rebuild their PageLsn (and the incremental-checkpoint
    /// redo-skip works) exactly as during normal operation.</summary>
    private void ReplayLogRecord(long lsn, BinaryReader reader)
    {
        var locator = DeserializeLocator(reader);
        var operations = locator.OperationsPersist.Read(reader);
        StampLsn(operations, lsn);
        Execute(operations);
    }

    /// <summary>Persists the current scheme (table/index definitions) to the heap and hardens it, so the
    /// op-log's locator ids always resolve on recovery. Called when a new locator is first logged.</summary>
    private void PersistScheme()
    {
        using (var ms = new MemoryStream())
        {
            _scheme.Serialize(new BinaryWriter(ms));
            _heap.Write(HANDLE_SCHEME, ms.GetBuffer(), 0, (int)ms.Length);
        }
        _heap.Commit();
    }

    /// <summary>
    /// The hook.
    /// </summary>
    public IOrderedSet<IData, IData> FindData(Locator originalLocator, Locator locator, IData key, Direction direction, out FullKey nearFullKey, out bool hasNearFullKey, ref FullKey lastVisitedFullKey)
    {
        if (_disposed)
            throw new ObjectDisposedException("WTree");

        nearFullKey = default(FullKey);
        hasNearFullKey = false;

        var branch = _rootBranch;
        branch.SyncRoot.Enter();
        // 'heldBranch' always tracks the branch whose lock we currently own.
        // The try/finally guarantees the lock is released even if an exception escapes.
        var heldBranch = branch;
        try
        {

        if (!_isRootCacheLoaded)
            LoadRootCache();

        Params param;
        if (key != null)
            param = new Params(WalkMethod.Cascade, WalkAction.None, null, true, locator, key);
        else
        {
            param = direction switch
            {
                Direction.Forward => new Params(WalkMethod.CascadeFirst, WalkAction.None, null, true, locator),
                Direction.Backward => new Params(WalkMethod.CascadeLast, WalkAction.None, null, true, locator),
                _ => throw new NotSupportedException(direction.ToString())
            };
        }

        branch.Fall(_depth + 1, new Token(CancellationToken.None), param);

        switch (direction)
        {
            case Direction.Forward:
                {
                    while (branch.NodeType == NodeType.Internal)
                    {
                        var newBranch = ((InternalNode)branch.Node).FindBranch(locator, key, direction, ref nearFullKey, ref hasNearFullKey);

                        newBranch.Value.SyncRoot.Enter();
                        var prevBranch = heldBranch;
                        heldBranch = newBranch.Value;
                        branch = newBranch.Value;
                        newBranch.Value.WaitFall();
                        Debug.Assert(!newBranch.Value.Cache.Contains(originalLocator));
                        prevBranch.SyncRoot.Exit();

                    }
                }
                break;
            case Direction.Backward:
                {
                    var depth = _depth;
                    var newBranch = default(KeyValuePair<FullKey, Branch>);
                    while (branch.NodeType == NodeType.Internal)
                    {
                        var node = (InternalNode)branch.Node;
                        newBranch = node.Branches[node.Branches.Count - 1];

                        var cmp = newBranch.Key.Locator.CompareTo(lastVisitedFullKey.Locator);
                        if (cmp == 0)
                        {
                            if (lastVisitedFullKey.Key == null)
                                cmp = -1;
                            else
                                cmp = newBranch.Key.Locator.KeyComparer.Compare(newBranch.Key.Key, lastVisitedFullKey.Key);
                        }

                        //newBranch.Key.CompareTo(lastVisitedFullKey) >= 0
                        if (cmp >= 0)
                            newBranch = node.FindBranch(locator, key, direction, ref nearFullKey, ref hasNearFullKey);
                        else
                        {
                            if (node.Branches.Count >= 2)
                            {
                                hasNearFullKey = true;
                                nearFullKey = node.Branches[node.Branches.Count - 2].Key;
                            }
                        }

                        newBranch.Value.SyncRoot.Enter();
                        // Update heldBranch immediately after acquiring the new lock so
                        // that the outer finally releases it if anything below throws.
                        var prevBranch = heldBranch;
                        heldBranch = newBranch.Value;
                        depth--;
                        newBranch.Value.WaitFall();
                        if (newBranch.Value.Cache.Contains(originalLocator))
                        {
                            newBranch.Value.Fall(depth + 1, new Token(CancellationToken.None), new Params(WalkMethod.Current, WalkAction.None, null, true, originalLocator));
                        }
                        Debug.Assert(!newBranch.Value.Cache.Contains(originalLocator));
                        prevBranch.SyncRoot.Exit();

                        branch = newBranch.Value;
                    }

                    //if (lastVisitedFullKey.Locator.Equals(newBranch.Key.Locator) &&
                    //    (lastVisitedFullKey.Key != null && lastVisitedFullKey.Locator.KeyEqualityComparer.Equals(lastVisitedFullKey.Key, newBranch.Key.Key)))
                    //{
                    //    Monitor.Exit(branch);
                    //    return null;
                    //}

                    lastVisitedFullKey = newBranch.Key;
                }
                break;
            default:
                throw new NotSupportedException(direction.ToString());
        }

        var data = ((LeafNode)branch.Node).FindData(originalLocator, direction, ref nearFullKey, ref hasNearFullKey);

        heldBranch.SyncRoot.Exit();
        heldBranch = null; // mark as released so the finally block skips it

        return data;

        } // end try
        finally
        {
            // Released in the success path above; only needed when an exception escapes.
            if (heldBranch != null)
                heldBranch.SyncRoot.Exit();
        }
    }

    private void Commit(CancellationToken cancellationToken, Locator locator = default(Locator), bool hasLocator = false, IData fromKey = null, IData toKey = null)
    {
        if (_disposed)
            throw new ObjectDisposedException("WTree");

        if (_operationLog != null)
        {
            // TransactionLog: the durable commit is a cheap sequential log fsync — NO node serialisation
            // under the root lock. Dirty nodes flush to the heap only at the occasional checkpoint.
            _operationLog.Commit(Volatile.Read(ref _lsn));
            if (CheckpointDue())
                CheckpointToHeap(cancellationToken, locator, hasLocator, fromKey, toKey);
            return;
        }

        CheckpointToHeap(cancellationToken, locator, hasLocator, fromKey, toKey);
    }

    /// <summary>Incremental-checkpoint selection (under the root lock, BEFORE the Store-fall). Only MARKS the
    /// nodes to flush this round — the root, every dirty internal node (few, high fan-out), and the coldest
    /// <see cref="_checkpointMaxNodes"/> dirty leaves (smallest MinDirtyLsn). NeverStored split products are
    /// flushed by the Store-fall gate directly (they always need an initial image), including ones the fall's
    /// Maintenance creates. The recovery LSN is computed AFTER the fall (see <see cref="ComputeIncrementalRecoveryLsn"/>)
    /// because Maintenance runs during the fall (it drains buffered ops down the tree — skipping it bloats
    /// nodes and freezes throughput) and can move ops between nodes, so a pre-fall LSN would be unsafe.</summary>
    private void SelectIncrementalCheckpoint()
    {
        List<Node> dirtyLeaves = null;

        foreach (var kv in _cache)
        {
            var node = kv.Value;
            if (!node.IsModified)
                continue;
            if (node.IsRoot || node.Type == NodeType.Internal)
                node.ToCheckpoint = true;   // root + internals are few — always flush (cheap, advances cpLsn)
            else
                (dirtyLeaves ??= new List<Node>()).Add(node);
        }

        if (dirtyLeaves != null)
        {
            // Flush the coldest CheckpointMaxNodes (oldest unflushed → advances the boundary most); the
            // remainder stay dirty and pin cpLsn just below their oldest unflushed op.
            if (dirtyLeaves.Count > _checkpointMaxNodes)
                dirtyLeaves.Sort(static (a, b) => a.MinDirtyLsn.CompareTo(b.MinDirtyLsn));
            for (var i = 0; i < dirtyLeaves.Count && i < _checkpointMaxNodes; i++)
                dirtyLeaves[i].ToCheckpoint = true;
        }

#if PERFORMANCE_CHECK
        if (dirtyLeaves != null)
            General.Diagnostics.PerformanceCheck.Observe("wtree.cp.incr.dirtyleaves", dirtyLeaves.Count);
#endif
    }

    /// <summary>Computes the incremental recovery boundary AFTER the Store-fall (Maintenance has settled and
    /// moved ops to their final nodes) and clears the per-pass ToCheckpoint marks. A node flushed this pass
    /// (ToCheckpoint, or NeverStored → stored by the gate) becomes durable, so it does not constrain the
    /// boundary; every other still-dirty node pins it. cpLsn = (min MinDirtyLsn over the still-non-durable
    /// dirty nodes) − 1, so every op ≤ cpLsn is durable (it lives only in flushed/clean nodes) and recovery
    /// replays strictly after it (idempotently). Safe regardless of how Maintenance reshaped the tree,
    /// because it reads the post-fall MinDirtyLsn (which reflects any sink that lowered a node's oldest op).</summary>
    private long ComputeIncrementalRecoveryLsn()
    {
        var recoveryLsn = long.MaxValue;
        foreach (var kv in _cache)
        {
            var node = kv.Value;
            if (node.ToCheckpoint || node.NeverStored)
            {
                node.ToCheckpoint = false;   // will be durable after FinalizeCommit — clear the per-pass mark
                continue;
            }
            if (node.IsModified && node.MinDirtyLsn < recoveryLsn)
                recoveryLsn = node.MinDirtyLsn;
        }
        return recoveryLsn == long.MaxValue ? Volatile.Read(ref _lsn) : recoveryLsn - 1;
    }

    private bool CheckpointDue()
        => Environment.TickCount64 - _lastCheckpointTicks >= _checkpointIntervalMs
           || _operationLog!.SizeBytes >= _checkpointLogSizeBytes;

    /// <summary>Serialises all dirty nodes to the heap and hardens it (the only place nodes hit disk). For
    /// TransactionLog this is the occasional checkpoint that advances the recovery boundary and truncates
    /// the log; for InPlace/WriteAheadLog it is every commit (historical behaviour).</summary>
    private void CheckpointToHeap(CancellationToken cancellationToken, Locator locator = default(Locator), bool hasLocator = false, IData fromKey = null, IData toKey = null)
    {
        _incrementalActiveCheckpoint = _incrementalCheckpoint && _operationLog != null;

        // Maintenance MUST run during the Store-fall (both full and incremental): it drains buffered ops down
        // the tree. Skipping it (an earlier incremental experiment) left ops piling into single nodes — 16k+
        // ops/node, ~1 MB nodes, multi-GB DB, 4 GB heap, GC-thrash freeze. So no NoMaintenance here; the
        // incremental recovery LSN is instead computed AFTER the fall, when the tree has settled.
        const WalkAction storeAction = WalkAction.Store;
        Params param;
        if (!hasLocator)
            param = new Params(WalkMethod.CascadeButOnlyLoaded, storeAction, null, false);
        else
        {
            if (fromKey == null)
                param = new Params(WalkMethod.CascadeButOnlyLoaded, storeAction, null, false, locator);
            else
            {
                if (toKey == null)
                    param = new Params(WalkMethod.CascadeButOnlyLoaded, storeAction, null, false, locator, fromKey);
                else
                    param = new Params(WalkMethod.CascadeButOnlyLoaded, storeAction, null, false, locator, fromKey, toKey);
            }
        }

        // Before the root lock: a deferred strategy waits for its previous background checkpoint here, so
        // this commit's Fall never mutates a node that is still being serialised.
        _commitStrategy.BeginCommit();

        _rootBranch.SyncRoot.Enter();
#if PERFORMANCE_CHECK
        var commitHoldStart = System.Diagnostics.Stopwatch.GetTimestamp();
#endif
        // Capture the checkpoint boundary under the lock: every op with lsn ≤ cpLsn will be reflected in
        // the nodes stored below, so recovery can replay strictly after it. Persisted via Settings v2.
        long cpLsn = 0;

        try
        {
            if (!_isRootCacheLoaded)
                LoadRootCache();

            if (_operationLog != null)
            {
                // Full checkpoint stores every dirty node → the whole log up to _lsn becomes durable.
                // Incremental flushes only a bounded subset: SelectIncrementalCheckpoint MARKS that subset
                // (ToCheckpoint) before the fall; the boundary is computed AFTER the fall (below).
                if (_incrementalActiveCheckpoint)
                    SelectIncrementalCheckpoint();
                else
                {
                    cpLsn = Volatile.Read(ref _lsn);
                    _checkpointLsn = cpLsn;
                }
            }

            var token = new Token(cancellationToken);
#if PERFORMANCE_CHECK
            var fallStart = System.Diagnostics.Stopwatch.GetTimestamp();
#endif
            _rootBranch.Fall(_depth + 1, token, param);
#if PERFORMANCE_CHECK
            General.Diagnostics.PerformanceCheck.ObserveDurationTicks("wtree.cp.fallstore", fallStart);
#endif

            if (_incrementalActiveCheckpoint)
            {
                // Post-fall: Maintenance has settled; flushed (ToCheckpoint/NeverStored) nodes are durable.
                cpLsn = ComputeIncrementalRecoveryLsn();
                _checkpointLsn = cpLsn;
            }

            //write settings
            using (var ms = new MemoryStream())
            {
                Settings.Serialize(this, ms);
                _heap.Write(HANDLE_SETTINGS, ms.GetBuffer(), 0, (int)ms.Length);
            }

            //write scheme
            using (var ms = new MemoryStream())
            {
                _scheme.Serialize(new BinaryWriter(ms));
                _heap.Write(HANDLE_SCHEME, ms.GetBuffer(), 0, (int)ms.Length);
            }

            //write root cache
            using (var ms = new MemoryStream())
            {
                _rootBranch.Cache.Store(this, new BinaryWriter(ms));
                _heap.Write(HANDLE_ROOT, ms.GetBuffer(), 0, (int)ms.Length);
            }

            // Evict cold nodes AFTER storing — all dirty nodes are now on disk,
            // so evicted nodes can be safely unloaded without data loss.
#if PERFORMANCE_CHECK
            var evictStart = System.Diagnostics.Stopwatch.GetTimestamp();
#endif
            EvictCache();
#if PERFORMANCE_CHECK
            General.Diagnostics.PerformanceCheck.ObserveDurationTicks("wtree.cp.evict", evictStart);
            General.Diagnostics.PerformanceCheck.Observe("gc.heap.mb", GC.GetTotalMemory(false) / (1024 * 1024));
            var finStart = System.Diagnostics.Stopwatch.GetTimestamp();
#endif

            _commitStrategy.FinalizeCommit(_heap);
#if PERFORMANCE_CHECK
            General.Diagnostics.PerformanceCheck.ObserveDurationTicks("wtree.cp.heapcommit", finStart);
#endif
        }
        finally
        {
#if PERFORMANCE_CHECK
            General.Diagnostics.PerformanceCheck.ObserveDurationTicks("wtree.commit.hold", commitHoldStart);
#endif
            _incrementalActiveCheckpoint = false;
            _rootBranch.SyncRoot.Exit();
        }

        // After the heap is durable (header swapped, incl. the new checkpoint LSN), drop the now-redundant
        // log prefix. Done outside the root lock — it touches only the log file.
        if (_operationLog != null)
        {
            _operationLog.Truncate(cpLsn);
            _lastCheckpointTicks = Environment.TickCount64;
        }

        CompactLohIfFragmented();
    }

    private long _lastLohCompactTicks = Environment.TickCount64;

    /// <summary>
    /// Memory safety net. Node store/load buffers are large <c>byte[]</c> (nodes are tens of KB to over a MB),
    /// which land on the Large Object Heap. The LOH is NOT compacted by default, so this churn fragments the
    /// heap — committed memory climbs unbounded (multi-GB <c>FragmentedBytes</c>) over hours until the process
    /// OOMs. When fragmentation crosses a high-water mark, force a one-shot compacting gen2 GC to return the
    /// holes to the OS. Rate-limited to at most once a minute so the blocking collection is rare (the cost is
    /// an occasional stall instead of an eventual crash). The proper fix is pooling the buffers so the large
    /// arrays are reused and never churn the LOH; this bounds memory until that lands.
    /// </summary>
    private void CompactLohIfFragmented()
    {
        if (Environment.TickCount64 - _lastLohCompactTicks < 60_000)
            return;

        var fragmentedBytes = GC.GetGCMemoryInfo().FragmentedBytes;
        if (fragmentedBytes < 768L * 1024 * 1024)   // only when the heap holds >768 MB of holes
            return;

        _lastLohCompactTicks = Environment.TickCount64;
        System.Runtime.GCSettings.LargeObjectHeapCompactionMode = System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
    }

    public virtual void Commit()
    {
        Commit(CancellationToken.None);
    }

    public IHeap Heap => _heap;

    #region Locator

    private Locator MinLocator => Locator.Min;

    protected internal Locator CreateLocator(string name, int structureType, DataType keyDataType, DataType recordDataType, Type keyType, Type recordType)
    {
        return _scheme.Create(name, structureType, keyDataType, recordDataType, keyType, recordType);
    }

    protected Locator GetLocator(long id)
    {
        return _scheme[id];
    }

    protected internal IEnumerable<Locator> GetAllLocators()
    {
        return _scheme.Select(kv => kv.Value);
    }

    private void SerializeLocator(BinaryWriter writer, Locator locator)
    {
        writer.Write(locator.Id);
    }

    private Locator DeserializeLocator(BinaryReader reader)
    {
        var id = reader.ReadInt64();
        if (id == Locator.Min.Id)
            return Locator.Min;

        var locator = _scheme[id];

        if (!locator.IsReady)
            locator.Prepare();

        if (locator == null)
            throw new Exception("Logical error");

        return locator;
    }

    #endregion

    #region Cache

    /// <summary>
    /// Branch.NodeID -> node
    /// </summary>
    private readonly ConcurrentDictionary<long, Node> _cache = new();

    private int _cacheSize = 4096;

    public int CacheSize
    {
        get => _cacheSize;
        set
        {
            if (value <= 0)
                throw new ArgumentException("Cache size is invalid.");

            _cacheSize = value;
        }
    }

    private void Packet(long id, Node node)
    {
        Debug.Assert(!_cache.ContainsKey(id));
        _cache[id] = node;
    }

    private Node Retrieve(long id)
    {
        _cache.TryGetValue(id, out var node);

        return node;
    }

    private Node Exclude(long id)
    {
        _cache.TryRemove(id, out var node);
        //Debug.Assert(node != null);

        return node;
    }

    /// <summary>
    /// Evicts cold nodes from the cache. Called synchronously during Commit
    /// (already under root lock) — no background thread, no races.
    ///
    /// Default policy is a <b>byte budget</b> (<see cref="DatabaseOptions.CacheSizeBytes"/>): WaterfallTree
    /// internal nodes carry message buffers and range from tens of KB to over a MB, so a fixed node
    /// <i>count</i> lets the managed heap — and GC pause time — grow without bound, which surfaces as
    /// throughput that decays the longer the process runs. Bounding by total bytes keeps the heap flat.
    /// A zero budget falls back to the legacy node-count policy.
    /// </summary>
    private void EvictCache()
    {
        var marked = _cacheSizeBytes > 0 ? MarkColdByBytes() : MarkColdByCount();

#if PERFORMANCE_CHECK
        General.Diagnostics.PerformanceCheck.Observe("wtree.cache.count", _cache.Count);
#endif
        if (!marked)
            return;

        // CacheFlush walk — already under root lock from Commit.
        // DoFall will Store+Unload each IsExpiredFromCache node.
        var token = new Token(CancellationToken.None);
        var param = new Params(WalkMethod.CascadeButOnlyLoaded, WalkAction.CacheFlush, null, false);
        _rootBranch.Fall(_depth + 1, token, param);
    }

    // A node's live managed footprint (deserialized op-collections, boxed Data, dictionaries) is several
    // times its serialized size; this factor turns the serialized estimate into an approximate-RAM figure
    // so CacheSizeBytes can be expressed in real memory terms.
    private const long InMemoryOverheadFactor = 8;

    /// <summary>Marks coldest nodes for eviction until the estimated cache RAM falls under the budget.</summary>
    private bool MarkColdByBytes()
    {
        long totalSerialized = 0;
        var count = 0;
        foreach (var kv in _cache)
        {
            if (kv.Value.IsRoot) continue;
            totalSerialized += kv.Value.ApproxByteSize;
            count++;
        }

        var totalRam = totalSerialized * InMemoryOverheadFactor;

#if PERFORMANCE_CHECK
        General.Diagnostics.PerformanceCheck.Observe("wtree.cache.bytes.mb", totalRam / (1024 * 1024));
#endif
        if (totalRam <= _cacheSizeBytes || count == 0)
            return false;

        // Sort coldest-first and evict until under budget (only when over — no alloc on the common path).
        var nodes = new (long TouchId, Node Node, long Ram)[count];
        var i = 0;
        foreach (var kv in _cache)
        {
            if (kv.Value.IsRoot) continue;
            nodes[i++] = (kv.Value.TouchId, kv.Value, kv.Value.ApproxByteSize * InMemoryOverheadFactor);
        }
        Array.Sort(nodes, static (a, b) => a.TouchId.CompareTo(b.TouchId));

        long freed = 0;
        var marked = 0;
        foreach (var (_, node, ram) in nodes)
        {
            if (totalRam - freed <= _cacheSizeBytes) break;
            node.IsExpiredFromCache = true;
            freed += ram;
            marked++;
        }
#if PERFORMANCE_CHECK
        General.Diagnostics.PerformanceCheck.Observe("wtree.evict.marked", marked);
#endif
        return marked > 0;
    }

    /// <summary>Legacy node-count eviction (used when the byte budget is disabled).</summary>
    private bool MarkColdByCount()
    {
        if (_cache.Count <= _cacheSize)
            return false;

        var evictCount = _cache.Count - _cacheSize;

        // O(n) partial select: find the evictCount nodes with lowest TouchId
        // without sorting the entire collection (avoids LINQ OrderBy allocation storm).
        var candidates = new (long TouchId, Node Node)[evictCount];
        var candidateCount = 0;
        long maxCandidateTouchId = long.MinValue;
        var maxCandidateIdx = 0;

        foreach (var kv in _cache)
        {
            var node = kv.Value;
            if (node.IsRoot)
                continue;

            var touchId = node.TouchId;

            if (candidateCount < evictCount)
            {
                candidates[candidateCount] = (touchId, node);
                if (touchId > maxCandidateTouchId)
                {
                    maxCandidateTouchId = touchId;
                    maxCandidateIdx = candidateCount;
                }
                candidateCount++;
            }
            else if (touchId < maxCandidateTouchId)
            {
                candidates[maxCandidateIdx] = (touchId, node);

                maxCandidateTouchId = long.MinValue;
                for (var i = 0; i < evictCount; i++)
                {
                    if (candidates[i].TouchId > maxCandidateTouchId)
                    {
                        maxCandidateTouchId = candidates[i].TouchId;
                        maxCandidateIdx = i;
                    }
                }
            }
        }

        for (var i = 0; i < candidateCount; i++)
            candidates[i].Node.IsExpiredFromCache = true;

        return candidateCount > 0;
    }

    #endregion

    #region IDisposable Members

    private void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                _workingFallCount.Wait();

                // Flush any in-flight background checkpoint (and join its worker) before closing the heap,
                // so a clean close is always fully durable even under CommitDurability.AsyncDeferred.
                _commitStrategy.Dispose();

                _heap.Close();
                _operationLog?.Dispose();
            }

            _disposed = true;
        }
    }

    public void Dispose()
    {
        Dispose(true);

        GC.SuppressFinalize(this);
    }

    ~WTree()
    {
        Dispose(false);
    }

    public virtual void Close()
    {
        Dispose();
    }

    #endregion

    public int GetMinimumlWTreeDepth(long recordCount)
    {
        var b = _internalNodeMaxBranches;
        var r = _internalNodeMaxOperationsInRoot;
        var I = _internalNodeMaxOperations;
        var l = _leafNodeMaxRecords;

        var depth = Math.Log(((recordCount - r) * (b - 1) + b * I) / (l * (b - 1) + I), b) + 1;

        return (int)Math.Ceiling(depth);
    }

    public int GetMaximumWTreeDepth(long recordCount)
    {
        var b = _internalNodeMaxBranches;
        var l = _leafNodeMaxRecords;

        var depth = Math.Log(recordCount / l, b) + 1;

        return (int)Math.Ceiling(depth);
    }
}

public enum Direction
{
    Backward = -1,
    None = 0,
    Forward = 1
}
