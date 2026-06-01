// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using System.Collections;
using System.Diagnostics;

namespace CatDb.WaterfallTree;
public partial class WTree
{
    private class BranchCache : IEnumerable<KeyValuePair<Locator, IOperationCollection>>
    {
        private Dictionary<Locator, IOperationCollection>? _cache;
        private IOperationCollection? _operations;

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

        public void Apply(IOperationCollection oprs, int startIndex, int count)
        {
            var operations = Obtain(oprs.Locator);

            operations.AddRange(oprs, startIndex, count);
            OperationCount += count;
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
                    
        public IOperationCollection? Exclude(Locator locator)
        {
            if (Count == 0)
                return null;

            IOperationCollection? operations;

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
            if (Count == 0)
                return EmptyEnumerator.Instance;

            if (Count == 1)
                return new SingleEnumerator(_operations.Locator, _operations);

            return new DictionaryEnumerator(_cache);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>Zero-allocation empty enumerator (singleton).</summary>
        private sealed class EmptyEnumerator : IEnumerator<KeyValuePair<Locator, IOperationCollection>>
        {
            public static readonly EmptyEnumerator Instance = new();
            public KeyValuePair<Locator, IOperationCollection> Current => default;
            object IEnumerator.Current => Current;
            public bool MoveNext() => false;
            public void Reset() { }
            public void Dispose() { }
        }

        /// <summary>Single-item enumerator — avoids array allocation for the common single-locator case.</summary>
        private struct SingleEnumerator : IEnumerator<KeyValuePair<Locator, IOperationCollection>>
        {
            private readonly KeyValuePair<Locator, IOperationCollection> _item;
            private bool _moved;

            public SingleEnumerator(Locator locator, IOperationCollection operations)
            {
                _item = new KeyValuePair<Locator, IOperationCollection>(locator, operations);
                _moved = false;
            }

            public KeyValuePair<Locator, IOperationCollection> Current => _item;
            object IEnumerator.Current => _item;
            public bool MoveNext() { if (_moved) return false; _moved = true; return true; }
            public void Reset() => _moved = false;
            public void Dispose() { }
        }

        /// <summary>Wraps Dictionary enumerator, converting System.Collections.Generic.KeyValuePair to CatDb.KeyValuePair.</summary>
        private struct DictionaryEnumerator : IEnumerator<KeyValuePair<Locator, IOperationCollection>>
        {
            private Dictionary<Locator, IOperationCollection>.Enumerator _inner;

            public DictionaryEnumerator(Dictionary<Locator, IOperationCollection> dict)
            {
                _inner = dict.GetEnumerator();
            }

            public KeyValuePair<Locator, IOperationCollection> Current
            {
                get
                {
                    var c = _inner.Current;
                    return new KeyValuePair<Locator, IOperationCollection>(c.Key, c.Value);
                }
            }

            object IEnumerator.Current => Current;
            public bool MoveNext() => _inner.MoveNext();
            public void Reset() { }
            public void Dispose() => _inner.Dispose();
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
