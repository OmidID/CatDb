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

        lock (_rootBranch)
        {
            if (!_isRootCacheLoaded)
                LoadRootCache();

            _rootBranch.ApplyToCache(operations);

            if (_rootBranch.Cache.OperationCount > _internalNodeMaxOperationsInRoot)
                Sink();
        }
    }

    public void Execute(Locator locator, IOperation operation)
    {
        if (_disposed)
            throw new ObjectDisposedException("WTree");

        lock (_rootBranch)
        {
            if (!_isRootCacheLoaded)
                LoadRootCache();

            _rootBranch.ApplyToCache(locator, operation);

            if (_rootBranch.Cache.OperationCount > _internalNodeMaxOperationsInRoot)
                Sink();
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
        Monitor.Enter(branch);
        // 'heldBranch' always tracks the branch whose Monitor lock we currently own.
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

                        Monitor.Enter(newBranch.Value);
                        var prevBranch = heldBranch;
                        heldBranch = newBranch.Value;
                        branch = newBranch.Value;
                        newBranch.Value.WaitFall();
                        Debug.Assert(!newBranch.Value.Cache.Contains(originalLocator));
                        Monitor.Exit(prevBranch);

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

                        Monitor.Enter(newBranch.Value);
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
                        Monitor.Exit(prevBranch);

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

        Monitor.Exit(heldBranch);
        heldBranch = null; // mark as released so the finally block skips it

        return data;

        } // end try
        finally
        {
            // Released in the success path above; only needed when an exception escapes.
            if (heldBranch != null)
                Monitor.Exit(heldBranch);
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

        lock (_rootBranch)
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

            _heap.Commit();
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
    /// </summary>
    private void EvictCache()
    {
        if (_cache.Count <= _cacheSize)
            return;

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
                // Replace the warmest candidate with this colder node
                candidates[maxCandidateIdx] = (touchId, node);

                // Rescan for new max (small array, fits in L1 cache)
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

        // CacheFlush walk — already under root lock from Commit.
        // DoFall will Store+Unload each IsExpiredFromCache node.
        var token = new Token(CancellationToken.None);
        var param = new Params(WalkMethod.CascadeButOnlyLoaded, WalkAction.CacheFlush, null, false);
        _rootBranch.Fall(_depth + 1, token, param);
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
