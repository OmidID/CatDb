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

    public WTree(IHeap heap, DatabaseOptions? options = null)
    {
        if (heap == null)
            throw new NullReferenceException("heap");

        _heap = heap;

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

        if (heap.Exists(HANDLE_SETTINGS))
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

            _rootBranch.ApplyToCache(locator, operation);

            if (_rootBranch.Cache.OperationCount > _internalNodeMaxOperationsInRoot)
                Sink();
        }
        finally
        {
            _rootBranch.SyncRoot.Exit();
        }
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

        Params param;
        if (!hasLocator)
            param = new Params(WalkMethod.CascadeButOnlyLoaded, WalkAction.Store, null, false);
        else
        {
            if (fromKey == null)
                param = new Params(WalkMethod.CascadeButOnlyLoaded, WalkAction.Store, null, false, locator);
            else
            {
                if (toKey == null)
                    param = new Params(WalkMethod.CascadeButOnlyLoaded, WalkAction.Store, null, false, locator, fromKey);
                else
                    param = new Params(WalkMethod.CascadeButOnlyLoaded, WalkAction.Store, null, false, locator, fromKey, toKey);
            }
        }

        _rootBranch.SyncRoot.Enter();
#if PERFORMANCE_CHECK
        var commitHoldStart = System.Diagnostics.Stopwatch.GetTimestamp();
#endif
        try
        {
            if (!_isRootCacheLoaded)
                LoadRootCache();

            var token = new Token(cancellationToken);
            _rootBranch.Fall(_depth + 1, token, param);

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
            EvictCache();

#if PERFORMANCE_CHECK
            General.Diagnostics.PerformanceCheck.Observe("gc.heap.mb", GC.GetTotalMemory(false) / (1024 * 1024));
#endif

            _heap.Commit();
        }
        finally
        {
#if PERFORMANCE_CHECK
            General.Diagnostics.PerformanceCheck.ObserveDurationTicks("wtree.commit.hold", commitHoldStart);
#endif
            _rootBranch.SyncRoot.Exit();
        }
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

                _heap.Close();
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
