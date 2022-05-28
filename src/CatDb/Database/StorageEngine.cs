using CatDb.Data;
using CatDb.WaterfallTree;
//using System.Management;
using System.Collections;
using System.Diagnostics;

namespace CatDb.Database
{
    public class StorageEngine : WTree, IStorageEngine
    {
        //user scheme
        private readonly Dictionary<string, Item1> _map = new();

        private readonly object _syncRoot = new();

        public StorageEngine(IHeap heap)
            : base(heap)
        {
            foreach (var locator in GetAllLocators())
            {
                if (locator.IsDeleted)
                    continue;
                
                var item = new Item1(locator, null);

                _map[locator.Name] = item;
            }
        }

        private Item1 Obtain(string name, int structureType, DataType keyDataType, DataType recordDataType, Type keyType, Type recordType)
        {
            Debug.Assert(keyDataType != null);
            Debug.Assert(recordDataType != null);

            if (!_map.TryGetValue(name, out var item))
            {
                if (keyType == null)
                    keyType = DataTypeUtils.BuildType(keyDataType);
                if (recordType == null)
                    recordType = DataTypeUtils.BuildType(recordDataType);

                var locator = CreateLocator(name, structureType, keyDataType, recordDataType, keyType, recordType);
                var table = new XTablePortable(this, locator);

                _map[name] = item = new Item1(locator, table);
            }
            else
            {
                var locator = item.Locator;

                if (locator.StructureType != structureType)
                    throw new ArgumentException($"Invalid structure type for '{name}'");

                if (keyDataType != locator.KeyDataType)
                    throw new ArgumentException("keyDataType");

                if (recordDataType != locator.RecordDataType)
                    throw new ArgumentException("recordDataType");

                if (locator.KeyType == null)
                    locator.KeyType = DataTypeUtils.BuildType(keyDataType);
                else
                {
                    if (keyType != null && keyType != locator.KeyType)
                        throw new ArgumentException($"Invalid keyType for table '{name}'");
                }

                if (locator.RecordType == null)
                    locator.RecordType = DataTypeUtils.BuildType(recordDataType);
                else
                {
                    if (recordType != null && recordType != locator.RecordType)
                        throw new ArgumentException($"Invalid recordType for table '{name}'");
                }

                locator.AccessTime = DateTime.Now;
            }

            if (!item.Locator.IsReady)
                item.Locator.Prepare();

            if (item.Table == null)
                item.Table = new XTablePortable(this, item.Locator);

            return item;
        }

        #region IStorageEngine

        public ITable<IData, IData> OpenXTablePortable(string name, DataType keyDataType, DataType recordDataType)
        {
            lock (_syncRoot)
            {
                var item = Obtain(name, StructureType.XTABLE, keyDataType, recordDataType, null, null);

                return item.Table;
            }
        }

        public ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string name, DataType keyDataType, DataType recordDataType, ITransformer<TKey, IData> keyTransformer, ITransformer<TRecord, IData> recordTransformer)
        {
            lock (_syncRoot)
            {
                var item = Obtain(name, StructureType.XTABLE, keyDataType, recordDataType, null, null);

                if (item.Portable == null)
                    item.Portable = new XTablePortable<TKey, TRecord>(item.Table, keyTransformer, recordTransformer);

                return (ITable<TKey, TRecord>)item.Portable;
            }
        }

        public ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string name)
        {
            var keyDataType = DataTypeUtils.BuildDataType(typeof(TKey));
            var recordDataType = DataTypeUtils.BuildDataType(typeof(TRecord));

            return OpenXTablePortable<TKey, TRecord>(name, keyDataType, recordDataType, null, null);
        }

        public ITable<TKey, TRecord> OpenXTable<TKey, TRecord>(string name)
        {
            lock (_syncRoot)
            {
                var keyType = typeof(TKey);
                var recordType = typeof(TRecord);

                var keyDataType = DataTypeUtils.BuildDataType(keyType);
                var recordDataType = DataTypeUtils.BuildDataType(recordType);

                var item = Obtain(name, StructureType.XTABLE, keyDataType, recordDataType, keyType, recordType);

                if (item.Direct == null)
                    item.Direct = new XTable<TKey, TRecord>(item.Table);

                return (XTable<TKey, TRecord>)item.Direct;
            }
        }

        public XFile OpenXFile(string name)
        {
            lock (_syncRoot)
            {
                var item = Obtain(name, StructureType.XFILE, DataType.Int64, DataType.ByteArray, typeof(long), typeof(byte[]));

                if (item.File == null)
                    item.File = new XFile(item.Table);

                return item.File;
            }
        }

        public IDescriptor this[string name]
        {
            get
            {
                lock (_syncRoot)
                {
                    if (!_map.TryGetValue(name, out var item))
                        return null;

                    return item.Locator;
                }
            }
        }

        public IDescriptor Find(long id)
        {
            lock (_syncRoot)
                return GetLocator(id);
        }

        public void Delete(string name)
        {
            lock (_syncRoot)
            {
                if (!_map.TryGetValue(name, out var item))
                    return;

                _map.Remove(name);

                if (item.Table != null)
                {
                    item.Table.Clear();
                    item.Table.Flush();
                }

                item.Locator.IsDeleted = true;
            }
        }

        public void Rename(string name, string newName)
        {
            lock (_syncRoot)
            {
                if (_map.ContainsKey(newName))
                    return;

                if (!_map.TryGetValue(name, out var item))
                    return;

                item.Locator.Name = newName;

                _map.Remove(name);
                _map.Add(newName, item);
            }
        }

        public bool Exists(string name)
        {
            lock (_syncRoot)
                return _map.ContainsKey(name);
        }

        public int Count
        {
            get
            {
                lock (_syncRoot)
                    return _map.Count;
            }
        }

        public override void Commit()
        {
            lock (_syncRoot)
            {
                foreach (var kv in _map)
                {
                    var table = kv.Value.Table;

                    if (table != null)
                    {
                        if (table.IsModified)
                            table.Locator.ModifiedTime = DateTime.Now;

                        table.Flush();
                    }
                }

                base.Commit();

                foreach (var kv in _map)
                {
                    var table = kv.Value.Table;

                    if (table != null)
                        table.IsModified = false;
                }
            }
        }

        public override void Close()
        {
            base.Close();
        }

        public IEnumerator<IDescriptor> GetEnumerator()
        {
            lock (_syncRoot)
            {
                return _map.Select(x => (IDescriptor)x.Value.Locator).GetEnumerator();
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        private class Item1
        {
            public readonly Locator Locator;
            public XTablePortable Table;

            public ITable Direct;
            public ITable Portable;
            public XFile File;            

            public Item1(Locator locator, XTablePortable table)
            {
                Locator = locator;
                Table = table;
            }
        }
    }
}
