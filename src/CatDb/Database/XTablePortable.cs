﻿using System.Collections;
using CatDb.General.Collections;
using CatDb.Data;
using CatDb.Database.Operations;
using CatDb.WaterfallTree;

namespace CatDb.Database
{
    public class XTablePortable : ITable<IData, IData>
    {
        private IOperationCollection _operations;

        public readonly WTree Tree;
        public readonly Locator Locator;
        public volatile bool IsModified;

        public readonly object SyncRoot = new();

        //public event Apply.ReadOperationDelegate PendingRead;

        internal XTablePortable(WTree tree, Locator locator)
        {
            Tree = tree;
            Locator = locator;

            _operations = locator.OperationCollectionFactory.Create(256);

            //((Apply)Path.DataDescriptor.Apply).ReadCallback += new Apply.ReadOperationDelegate(Apply_ReadCallback);
        }

        //~XIndex()
        //{
        //    Flush();
        //}

        //private void Apply_ReadCallback(long handle, bool exist, Path path, IKey key, IRecord record)
        //{
        //    if (!Path.Equals(path))
        //        return;

        //    if (PendingRead != null)
        //        PendingRead(handle, exist, path, key, record);
        //}

        //private void Read(IKey key, long handle)
        //{
        //    InternalExecute(new ReadOperation(key, handle));
        //}

        private void Execute(IOperation operation)
        {
            lock (SyncRoot)
            {
                IsModified = true;

                if (_operations.Capacity == 0)
                {
                    Tree.Execute(Locator, operation);
                    return;
                }

                _operations.Add(operation);
                if (_operations.Count == _operations.Capacity)
                    Flush();
            }
        }

        public void Flush()
        {
            lock (SyncRoot)
            {
                if (_operations.Count == 0)
                    return;

                Tree.Execute(_operations);

                _operations.Clear();
            }
        }

        #region ITable<IKey, IRecord>

        public IData this[IData key]
        {
            get
            {
                if (!TryGet(key, out var record))
                    throw new KeyNotFoundException(key.ToString());

                return record;
            }
            set => Replace(key, value);
        }

        public void Replace(IData key, IData record)
        {
            Execute(new ReplaceOperation(key, record));
        }

        public void InsertOrIgnore(IData key, IData record)
        {
            Execute(new InsertOrIgnoreOperation(key, record));
        }

        public void Delete(IData key)
        {
            Execute(new DeleteOperation(key));
        }

        public void Delete(IData fromKey, IData toKey)
        {
            Execute(new DeleteRangeOperation(fromKey, toKey));
        }

        public void Clear()
        {
            Execute(new ClearOperation());
        }

        public bool Exists(IData key)
        {
            return TryGet(key, out _);
        }

        public bool TryGet(IData key, out IData record)
        {
            lock (SyncRoot)
            {
                Flush();

                var lastVisitedFullKey = default(WTree.FullKey);

                var records = Tree.FindData(Locator, Locator, key, Direction.Forward, out _, out _, ref lastVisitedFullKey);
                if (records == null)
                {
                    record = default(IData);
                    return false;
                }

                lock (records)
                {
                    return records.TryGetValue(key, out record);
                }
            }
        }

        public IData Find(IData key)
        {
            TryGet(key, out var record);

            return record;
        }

        public IData TryGetOrDefault(IData key, IData defaultRecord)
        {
            if (!TryGet(key, out var record))
                return defaultRecord;

            return record;
        }

        public KeyValuePair<IData, IData>? FindNext(IData key)
        {
            lock (SyncRoot)
            {
                foreach (var kv in Forward(key, true, default(IData), false))
                    return kv;

                return null;
            }
        }

        public KeyValuePair<IData, IData>? FindAfter(IData key)
        {
            lock (SyncRoot)
            {
                var comparer = Locator.KeyComparer;

                foreach (var kv in Forward(key, true, default(IData), false))
                {
                    if (comparer.Compare(kv.Key, key) == 0)
                        continue;

                    return kv;
                }

                return null;
            }
        }

        public KeyValuePair<IData, IData>? FindPrev(IData key)
        {
            lock (SyncRoot)
            {
                foreach (var kv in Backward(key, true, default(IData), false))
                    return kv;

                return null;
            }
        }

        public KeyValuePair<IData, IData>? FindBefore(IData key)
        {
            lock (SyncRoot)
            {
                var comparer = Locator.KeyComparer;

                foreach (var kv in Backward(key, true, default(IData), false))
                {
                    if (comparer.Compare(kv.Key, key) == 0)
                        continue;

                    return kv;
                }

                return null;
            }
        }

        public IEnumerable<KeyValuePair<IData, IData>> Forward()
        {
            return Forward(default(IData), false, default(IData), false);
        }

        public IEnumerable<KeyValuePair<IData, IData>> Forward(IData from, bool hasFrom, IData to, bool hasTo)
        {
            lock (SyncRoot)
            {
                var keyComparer = Locator.KeyComparer;

                if (hasFrom && hasTo && keyComparer.Compare(from, to) > 0)
                    throw new ArgumentException("from > to");

                Flush();

                var lastVisitedFullKey = default(WTree.FullKey);
                IOrderedSet<IData, IData> records;

                records = Tree.FindData(Locator, Locator, hasFrom ? from : null, Direction.Forward, out var nearFullKey, out var hasNearFullKey, ref lastVisitedFullKey);

                if (records == null)
                {
                    if (!hasNearFullKey || !nearFullKey.Locator.Equals(Locator))
                        yield break;

                    records = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key, Direction.Forward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);
                }

                while (records != null) // && records.Count > 0
                {
                    Task task = null;
                    IOrderedSet<IData, IData> recs = null;

                    if (hasNearFullKey && nearFullKey.Locator.Equals(Locator))
                    {
                        lock (records)
                        {
                            if (hasTo && records.Count > 0 && keyComparer.Compare(records.First.Key, to) > 0)
                                break;
                        }

                        task = Task.Factory.StartNew(() =>
                        {
                            recs = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key, Direction.Forward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);
                        });
                    }

                    lock (records)
                    {
                        foreach (var record in records.Forward(from, hasFrom, to, hasTo))
                            yield return record;
                    }

                    if (task != null)
                        task.Wait();

                    records = recs;
                }
            }
        }

        public IEnumerable<KeyValuePair<IData, IData>> Backward()
        {
            return Backward(default(IData), false, default(IData), false);
        }

        public IEnumerable<KeyValuePair<IData, IData>> Backward(IData to, bool hasTo, IData from, bool hasFrom)
        {
            lock (SyncRoot)
            {
                var keyComparer = Locator.KeyComparer;

                if (hasFrom && hasTo && keyComparer.Compare(from, to) > 0)
                    throw new ArgumentException("from > to");

                Flush();

                IOrderedSet<IData, IData> records;

                var lastVisitedFullKey = new WTree.FullKey(Locator, to);
                records = Tree.FindData(Locator, Locator, hasTo ? to : null, Direction.Backward, out var nearFullKey, out var hasNearFullKey, ref lastVisitedFullKey);

                if (records == null)
                    yield break;

                while (records != null)
                {
                    Task task = null;
                    IOrderedSet<IData, IData> recs = null;

                    //if (records.Count > 0)
                    //    lastVisitedFullKey = new WTree.FullKey(Locator, records.First.Key);

                    if (hasNearFullKey)
                    {
                        lock (records)
                        {
                            if (hasFrom && records.Count > 0 && keyComparer.Compare(records.Last.Key, from) < 0)
                                break;
                        }

                        task = Task.Factory.StartNew(() =>
                        {
                            recs = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key, Direction.Backward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);
                        });
                    }

                    lock (records)
                    {
                        foreach (var record in records.Backward(to, hasTo, from, hasFrom))
                            yield return record;
                    }

                    if (task != null)
                        task.Wait();

                    if (recs == null)
                        break;

                    lock (records)
                    {
                        lock (recs)
                        {
                            if (recs.Count > 0 && records.Count > 0)
                            {
                                if (keyComparer.Compare(recs.First.Key, records.First.Key) >= 0)
                                    break;
                            }
                        }
                    }

                    records = recs;
                }
            }
        }

        public KeyValuePair<IData, IData> FirstRow => Forward().First();

        public KeyValuePair<IData, IData> LastRow => Backward().First();

        public long Count()
        {
            return this.LongCount();
        }

        public IDescriptor Descriptor => Locator;

        public IEnumerator<KeyValuePair<IData, IData>> GetEnumerator()
        {
            return Forward().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        public int OperationQueueCapacity
        {
            get
            {
                lock (SyncRoot)
                    return _operations.Capacity;
            }
            set
            {
                lock (SyncRoot)
                {
                    Flush();

                    _operations = Locator.OperationCollectionFactory.Create(value);
                }
            }
        }
    }
}
