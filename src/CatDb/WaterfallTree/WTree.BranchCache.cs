using System.Collections;
using System.Diagnostics;

namespace CatDb.WaterfallTree
{
    public partial class WTree
    {
        private class BranchCache : IEnumerable<KeyValuePair<Locator, IOperationCollection>>
        {
            private Dictionary<Locator, IOperationCollection> _cache;
            private IOperationCollection _operations;

            /// <summary>
            /// Number of all operations in cache
            /// </summary>
            public int OperationCount { get; private set; }

            public int Count { get; private set; }

            public BranchCache()
            {
            }

            public BranchCache(IOperationCollection operations)
            {
                _operations = operations;
                Count = 1;
                OperationCount = operations.Count;
            }

            private IOperationCollection Obtain(Locator locator)
            {
                if (Count == 0)
                {
                    _operations = locator.OperationCollectionFactory.Create(0);
                    Debug.Assert(_cache == null);
                    Count++;
                }
                else
                {
                    if (!_operations.Locator.Equals(locator))
                    {
                        if (_cache == null)
                        {
                            _cache = new Dictionary<Locator, IOperationCollection>
                            {
                                [_operations.Locator] = _operations
                            };
                        }

                        if (!_cache.TryGetValue(locator, out _operations))
                        {
                            _cache[locator] = _operations = locator.OperationCollectionFactory.Create(0);
                            Count++;
                        }
                    }
                }

                return _operations;
            }

            public void Apply(Locator locator, IOperation operation)
            {
                var operations = Obtain(locator);

                operations.Add(operation);
                OperationCount++;
            }

            public void Apply(IOperationCollection oprs)
            {
                var operations = Obtain(oprs.Locator);

                operations.AddRange(oprs);
                OperationCount += oprs.Count;
            }

            public void Clear()
            {
                _cache = null;
                _operations = null;
                Count = 0;
                OperationCount = 0;
            }

            public bool Contains(Locator locator)
            {
                if (Count == 0)
                    return false;

                if (Count == 1)
                    return _operations.Locator.Equals(locator);

                if (_cache != null)
                    return _cache.ContainsKey(locator);

                return false;
            }
                        
            public IOperationCollection Exclude(Locator locator)
            {
                if (Count == 0)
                    return null;

                IOperationCollection operations;

                if (!_operations.Locator.Equals(locator))
                {
                    if (_cache == null || !_cache.TryGetValue(locator, out operations))
                        return null;

                    _cache.Remove(locator);
                    if (_cache.Count == 1)
                        _cache = null;
                }
                else
                {
                    operations = _operations;

                    if (Count == 1)
                        _operations = null;
                    else
                    {
                        _cache.Remove(locator);
                        _operations = _cache.First().Value;
                        if (_cache.Count == 1)
                            _cache = null;
                    }
                }

                Count--;
                OperationCount -= operations.Count;

                return operations;
            }

            public IEnumerator<KeyValuePair<Locator, IOperationCollection>> GetEnumerator()
            {
                IEnumerable<KeyValuePair<Locator, IOperationCollection>> enumerable;

                if (Count == 0)
                    enumerable = Enumerable.Empty<KeyValuePair<Locator, IOperationCollection>>();
                else if (Count == 1)
                    enumerable = new[] { new KeyValuePair<Locator, IOperationCollection>(_operations.Locator, _operations) };
                else
                    enumerable = _cache.Select(s => new KeyValuePair<Locator, IOperationCollection>(s.Key, s.Value));

                return enumerable.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public void Store(WTree tree, BinaryWriter writer)
            {
                writer.Write(Count);
                if (Count == 0)
                    return;

                //write cache
                foreach (var kv in this)
                {
                    var locator = kv.Key;
                    var operations = kv.Value;

                    //write locator
                    tree.SerializeLocator(writer, locator);

                    //write operations
                    locator.OperationsPersist.Write(writer, operations);
                }
            }

            public void Load(WTree tree, BinaryReader reader)
            {
                var count = reader.ReadInt32();
                if (count == 0)
                    return;

                for (var i = 0; i < count; i++)
                {
                    //read locator
                    var locator = tree.DeserializeLocator(reader);

                    //read operations
                    var operations = locator.OperationsPersist.Read(reader);

                    Add(locator, operations);
                }
            }

            private void Add(Locator locator, IOperationCollection operations)
            {
                if (Count > 0)
                {
                    if (_cache == null)
                    {
                        _cache = new Dictionary<Locator, IOperationCollection>
                        {
                            [_operations.Locator] = _operations
                        };
                    }

                    _cache.Add(locator, operations);
                }

                _operations = operations;

                OperationCount += operations.Count;
                Count++;
            }
        }
    }
}
