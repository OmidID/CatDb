#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using System.Collections;
using System.Diagnostics;
using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public class StorageEngine : WTree, IStorageEngine
{
    private readonly Dictionary<string, Item1> _map = new();
    private readonly object _syncRoot = new();

    public StorageEngine(IHeap heap) : base(heap)
    {
        foreach (var locator in GetAllLocators())
        {
            if (locator.IsDeleted)
                continue;

            _map[locator.Name] = new Item1(locator, null);
        }
    }

    private Item1 Obtain(string name, int structureType, DataType keyDataType, DataType recordDataType, Type keyType, Type recordType)
    {
        Debug.Assert(keyDataType != null);
        Debug.Assert(recordDataType != null);

        if (!_map.TryGetValue(name, out var item))
        {
            keyType    ??= DataTypeUtils.BuildType(keyDataType);
            recordType ??= DataTypeUtils.BuildType(recordDataType);

            var locator = CreateLocator(name, structureType, keyDataType, recordDataType, keyType, recordType);
            var table   = new XTablePortable(this, locator);
            item = new Item1(locator, table);
            _map[name] = item;
        }
        else
        {
            var locator = item.Locator;

            if (locator.StructureType != structureType)
                throw new ArgumentException($"Invalid structure type for '{name}'");
            if (keyDataType != locator.KeyDataType)
                throw new ArgumentException(nameof(keyDataType));
            if (recordDataType != locator.RecordDataType)
                throw new ArgumentException(nameof(recordDataType));

            locator.KeyType ??= DataTypeUtils.BuildType(keyDataType);
            if (keyType != null && keyType != locator.KeyType)
                throw new ArgumentException($"Invalid keyType for table '{name}'");

            locator.RecordType ??= DataTypeUtils.BuildType(recordDataType);
            if (recordType != null && recordType != locator.RecordType)
                throw new ArgumentException($"Invalid recordType for table '{name}'");

            locator.AccessTime = DateTime.Now;
        }

        if (!item.Locator.IsReady)
            item.Locator.Prepare();

        item.Table ??= new XTablePortable(this, item.Locator);

        return item;
    }

    public ITable<IData, IData> OpenXTablePortable(string name, DataType keyDataType, DataType recordDataType)
    {
        lock (_syncRoot)
            return Obtain(name, StructureType.XTABLE, keyDataType, recordDataType, null, null).Table;
    }

    public ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string name, DataType keyDataType, DataType recordDataType, ITransformer<TKey, IData> keyTransformer, ITransformer<TRecord, IData> recordTransformer)
    {
        lock (_syncRoot)
        {
            var item = Obtain(name, StructureType.XTABLE, keyDataType, recordDataType, null, null);
            item.Portable ??= new XTablePortable<TKey, TRecord>(item.Table, keyTransformer, recordTransformer);
            return (ITable<TKey, TRecord>)item.Portable;
        }
    }

    public ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string name)
    {
        var keyDataType    = DataTypeUtils.BuildDataType(typeof(TKey));
        var recordDataType = DataTypeUtils.BuildDataType(typeof(TRecord));
        return OpenXTablePortable<TKey, TRecord>(name, keyDataType, recordDataType, null, null);
    }

    public ITable<TKey, TRecord> OpenXTable<TKey, TRecord>(string name)
    {
        lock (_syncRoot)
        {
            var keyDataType    = DataTypeUtils.BuildDataType(typeof(TKey));
            var recordDataType = DataTypeUtils.BuildDataType(typeof(TRecord));
            var item = Obtain(name, StructureType.XTABLE, keyDataType, recordDataType, typeof(TKey), typeof(TRecord));
            item.Direct ??= new XTable<TKey, TRecord>(item.Table);
            return (XTable<TKey, TRecord>)item.Direct;
        }
    }

    public XFile OpenXFile(string name)
    {
        lock (_syncRoot)
        {
            var item = Obtain(name, StructureType.XFILE, DataType.Int64, DataType.ByteArray, typeof(long), typeof(byte[]));
            item.File ??= new XFile(item.Table);
            return item.File;
        }
    }

    public IDescriptor this[string name]
    {
        get
        {
            lock (_syncRoot)
                return _map.TryGetValue(name, out var item) ? item.Locator : null;
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

            if (item.Table is not null)
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
            if (_map.ContainsKey(newName) || !_map.TryGetValue(name, out var item))
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
        get { lock (_syncRoot) return _map.Count; }
    }

    public override void Commit()
    {
        lock (_syncRoot)
        {
            foreach (var kv in _map)
            {
                var table = kv.Value.Table;
                if (table is null) continue;

                if (table.IsModified)
                    table.Locator.ModifiedTime = DateTime.Now;

                table.Flush();
            }

            base.Commit();

            foreach (var kv in _map)
            {
                if (kv.Value.Table is not null)
                    kv.Value.Table.IsModified = false;
            }
        }
    }

    public override void Close() => base.Close();

    public IEnumerator<IDescriptor> GetEnumerator()
    {
        lock (_syncRoot)
            return _map.Select(x => (IDescriptor)x.Value.Locator).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public new void Dispose() => Close();

    private sealed class Item1(Locator locator, XTablePortable table)
    {
        public readonly Locator Locator = locator;
        public XTablePortable   Table   = table;
        public ITable           Direct;
        public ITable           Portable;
        public XFile            File;
    }
}
