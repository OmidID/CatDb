// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
using System.Collections;
using System.Diagnostics;
using CatDb.Data;
using CatDb.General.Threading;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public class StorageEngine : WTree, IStorageEngine
{
    private readonly Dictionary<string, Item1> _map = new();
    private readonly Dictionary<TransformerCacheKey, object> _transformerCache = new();
    private readonly ReentrantLock _syncRoot = new();

    private readonly record struct TransformerCacheKey(Type ObjectType, Type DataType);

    public StorageEngine(IHeap heap, DatabaseOptions? options = null) : base(heap, options)
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

        // Capture member names the first time a typed (non-anonymous) table is opened.
        // This persists slot-index → name mapping so the HTTP API can produce named JSON.
        item.Locator.CaptureMembers(item.Locator.KeyType, item.Locator.RecordType);

        item.Table ??= new XTablePortable(this, item.Locator);

        return item;
    }

    public ITable<IData, IData> OpenXTablePortable(string name, DataType keyDataType, DataType recordDataType)
    {
        _syncRoot.Enter();
        try { return Obtain(name, StructureType.XTABLE, keyDataType, recordDataType, null, null).Table; }
        finally { _syncRoot.Exit(); }
    }

    public ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string name, DataType keyDataType, DataType recordDataType, ITransformer<TKey, IData> keyTransformer, ITransformer<TRecord, IData> recordTransformer)
    {
        _syncRoot.Enter();
        try
        {
            var item = Obtain(name, StructureType.XTABLE, keyDataType, recordDataType, null, null);
            keyTransformer ??= GetOrCreateTransformer<TKey>(item.Locator.KeyType!);
            recordTransformer ??= GetOrCreateTransformer<TRecord>(item.Locator.RecordType!);
            item.Portable ??= new XTablePortable<TKey, TRecord>(item.Table, keyTransformer, recordTransformer);
            return (ITable<TKey, TRecord>)item.Portable;
        }
        finally { _syncRoot.Exit(); }
    }

    private ITransformer<T, IData> GetOrCreateTransformer<T>(Type dataType)
    {
        var key = new TransformerCacheKey(typeof(T), dataType);
        if (_transformerCache.TryGetValue(key, out var cached))
            return (ITransformer<T, IData>)cached;

        var transformer = new DataTransformer<T>(dataType);
        _transformerCache[key] = transformer;
        return transformer;
    }

    public ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string name)
    {
        var keyDataType    = DataTypeUtils.BuildDataType(typeof(TKey));
        var recordDataType = DataTypeUtils.BuildDataType(typeof(TRecord));
        return OpenXTablePortable<TKey, TRecord>(name, keyDataType, recordDataType, null, null);
    }

    public ITable<TKey, TRecord> OpenXTable<TKey, TRecord>(string name)
    {
        _syncRoot.Enter();
        try
        {
            var keyDataType    = DataTypeUtils.BuildDataType(typeof(TKey));
            var recordDataType = DataTypeUtils.BuildDataType(typeof(TRecord));
            var item = Obtain(name, StructureType.XTABLE, keyDataType, recordDataType, typeof(TKey), typeof(TRecord));
            item.Direct ??= new XTable<TKey, TRecord>(item.Table);
            return (XTable<TKey, TRecord>)item.Direct;
        }
        finally { _syncRoot.Exit(); }
    }

    public XFile OpenXFile(string name)
    {
        _syncRoot.Enter();
        try
        {
            var item = Obtain(name, StructureType.XFILE, DataType.Int64, DataType.ByteArray, typeof(long), typeof(byte[]));
            item.File ??= new XFile(item.Table);
            return item.File;
        }
        finally { _syncRoot.Exit(); }
    }

    public IDescriptor this[string name]
    {
        get
        {
            _syncRoot.Enter();
            try { return _map.TryGetValue(name, out var item) ? item.Locator : null; }
            finally { _syncRoot.Exit(); }
        }
    }

    public IDescriptor Find(long id)
    {
        _syncRoot.Enter();
        try { return GetLocator(id); }
        finally { _syncRoot.Exit(); }
    }

    public void Delete(string name)
    {
        _syncRoot.Enter();
        try
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
        finally { _syncRoot.Exit(); }
    }

    public void Rename(string name, string newName)
    {
        _syncRoot.Enter();
        try
        {
            if (_map.ContainsKey(newName) || !_map.TryGetValue(name, out var item))
                return;

            item.Locator.Name = newName;
            _map.Remove(name);
            _map.Add(newName, item);
        }
        finally { _syncRoot.Exit(); }
    }

    public bool Exists(string name)
    {
        _syncRoot.Enter();
        try { return _map.ContainsKey(name); }
        finally { _syncRoot.Exit(); }
    }

    public int Count
    {
        get
        {
            _syncRoot.Enter();
            try { return _map.Count; }
            finally { _syncRoot.Exit(); }
        }
    }

    public override void Commit()
    {
        _syncRoot.Enter();
        try
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
        finally { _syncRoot.Exit(); }
    }

    public override void Close() => base.Close();

    public IEnumerator<IDescriptor> GetEnumerator()
    {
        _syncRoot.Enter();
        try { return _map.Select(x => (IDescriptor)x.Value.Locator).GetEnumerator(); }
        finally { _syncRoot.Exit(); }
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
