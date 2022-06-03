using System.Collections.Concurrent;
using System.Diagnostics;
using CatDb.Data;
using CatDb.General.Collections;
using CatDb.General.Threading;

namespace CatDb.WaterfallTree
{
    public partial class WTree : IDisposable
    {
        private int _internalNodeMinBranches = 2; //default values
        private int _internalNodeMaxBranches = 5;
        private int _internalNodeMaxOperationsInRoot = 8 * 1024;
        private int _internalNodeMinOperations = 32 * 1024;
        private int _internalNodeMaxOperations = 64 * 1024;
        private int _leafNodeMinRecords = 8 * 1024;
        private int _leafNodeMaxRecords = 64 * 1024;

        //reserved handles
        private const long HANDLE_SETTINGS = 0;
        private const long HANDLE_SCHEME = 1;
        private const long HANDLE_ROOT = 2;
        private const long HANDLE_RESERVED = 3;

        private readonly Countdown _workingFallCount = new();
        private readonly Branch _rootBranch;
        private bool _isRootCacheLoaded;

        private volatile bool _disposed;
        private volatile bool _shutdown;
        private int _depth = 1;

        private long _globalVersion;

        public long GlobalVersion
        {
            get => Interlocked.Read(ref _globalVersion);
            set => Interlocked.Exchange(ref _globalVersion, value);
        }

        private readonly Scheme _scheme;
        private readonly IHeap _heap;

        public WTree(IHeap heap)
        {
            if (heap == null)
                throw new NullReferenceException("heap");

            _heap = heap;

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

            _cacheThread = new Thread(DoCache);
            _cacheThread.Start();
        }

        private void LoadRootCache()
        {
            using (var ms = new MemoryStream(_heap.Read(HANDLE_ROOT)))
                _rootBranch.Cache.Load(this, new BinaryReader(ms));

            _isRootCacheLoaded = true;
        }

        private void Sink()
        {
            _rootBranch.WaitFall();

            if (_rootBranch.NodeState != NodeState.None)
            {
                var token = new Token(_cacheSemaphore, new CancellationTokenSource().Token);
                _rootBranch.MaintenanceRoot(token);
                _rootBranch.Node.Touch(_depth + 1);
                token.CountdownEvent.Wait();
            }

            _rootBranch.Fall(_depth + 1, new Token(_cacheSemaphore, CancellationToken.None), new Params(WalkMethod.Current, WalkAction.None, null, true));
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

            branch.Fall(_depth + 1, new Token(_cacheSemaphore, CancellationToken.None), param);
            branch.WaitFall();

            switch (direction)
            {
                case Direction.Forward:
                    {
                        while (branch.NodeType == NodeType.Internal)
                        {
                            var newBranch = ((InternalNode)branch.Node).FindBranch(locator, key, direction, ref nearFullKey, ref hasNearFullKey);

                            Monitor.Enter(newBranch.Value);
                            newBranch.Value.WaitFall();
                            Debug.Assert(!newBranch.Value.Cache.Contains(originalLocator));
                            Monitor.Exit(branch);

                            branch = newBranch.Value;
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
                            //else
                            //{
                            //    Debug.WriteLine("");
                            //}

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
                            depth--;
                            newBranch.Value.WaitFall();
                            if (newBranch.Value.Cache.Contains(originalLocator))
                            {
                                newBranch.Value.Fall(depth + 1, new Token(_cacheSemaphore, CancellationToken.None), new Params(WalkMethod.Current, WalkAction.None, null, true, originalLocator));
                                newBranch.Value.WaitFall();
                            }
                            Debug.Assert(!newBranch.Value.Cache.Contains(originalLocator));
                            Monitor.Exit(branch);

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

            Monitor.Exit(branch);

            return data;
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
                
                var token = new Token(_cacheSemaphore, cancellationToken);
                _rootBranch.Fall(_depth + 1, token, param);

                token.CountdownEvent.Signal();
                token.CountdownEvent.Wait();

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

        protected Locator CreateLocator(string name, int structureType, DataType keyDataType, DataType recordDataType, Type keyType, Type recordType)
        {
            return _scheme.Create(name, structureType, keyDataType, recordDataType, keyType, recordType);
        }

        protected Locator GetLocator(long id)
        {
            return _scheme[id];
        }

        protected IEnumerable<Locator> GetAllLocators()
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
        private readonly Thread _cacheThread;

        private SemaphoreSlim _cacheSemaphore = new(int.MaxValue, int.MaxValue);

        private int _cacheSize = 32;

        public int CacheSize
        {
            get => _cacheSize;
            set
            {
                if (value <= 0)
                    throw new ArgumentException("Cache size is invalid.");

                _cacheSize = value;

                if (_cache.Count > CacheSize * 1.1)
                {
                    lock (_cache)
                        Monitor.Pulse(_cache);
                }
            }
        }

        private void Packet(long id, Node node)
        {
            Debug.Assert(!_cache.ContainsKey(id));
            _cache[id] = node;

            if (_cache.Count > CacheSize * 1.1)
            {
                lock (_cache)
                    Monitor.Pulse(_cache);
            }
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

            var delta = (int)(CacheSize * 1.1 - _cache.Count);
            if (delta > 0)
                _cacheSemaphore.Release(delta);

            return node;
        }

        private void DoCache()
        {
            while (!_shutdown)
            {
                while (_cache.Count > CacheSize * 1.1)
                {
                    var kvs = _cache.Select(s => new KeyValuePair<long, Node>(s.Key, s.Value)).ToArray();

                    foreach (var kv in kvs.Where(x => !x.Value.IsRoot).OrderBy(x => x.Value.TouchId).Take(_cache.Count - CacheSize))
                        kv.Value.IsExpiredFromCache = true;
                    //Debug.WriteLine(Cache.Count);
                    Token token;
                    lock (_rootBranch)
                    {
                        token = new Token(_cacheSemaphore, CancellationToken.None);
                        _cacheSemaphore = new SemaphoreSlim(0, int.MaxValue);
                        var param = new Params(WalkMethod.CascadeButOnlyLoaded, WalkAction.CacheFlush, null, false);
                        _rootBranch.Fall(_depth + 1, token, param);
                    }

                    token.CountdownEvent.Signal();
                    token.CountdownEvent.Wait();
                    _cacheSemaphore.Release(int.MaxValue / 2);
                }

                lock (_cache)
                {
                    if (_cache.Count <= CacheSize * 1.1)
                        Monitor.Wait(_cache, 1);
                }
            }
        }

        #endregion

        #region IDisposable Members

        private void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _shutdown = true;
                    if (_cacheThread != null)
                        _cacheThread.Join();

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
}
