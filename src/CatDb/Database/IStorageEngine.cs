// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public interface IStorageEngine : IEnumerable<IDescriptor>, IDisposable
{
    /// Works with anonymous types.
    ITable<IData, IData> OpenXTablePortable(string name, DataType keyDataType, DataType recordDataType);

    /// Works with portable types via custom transformers.
    ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string name, DataType keyDataType, DataType recordDataType, ITransformer<TKey, IData> keyTransformer, ITransformer<TRecord, IData> recordTransformer);

    /// Works with anonymous types via default transformers.
    ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string name);

    /// Works with the user types directly.
    ITable<TKey, TRecord> OpenXTable<TKey, TRecord>(string name);

    XFile OpenXFile(string name);

    IDescriptor this[string name] { get; }
    IDescriptor Find(long id);

    void Delete(string name);
    void Rename(string name, string newName);
    bool Exists(string name);

    /// The number of tables &amp; virtual files in the storage engine.
    int Count { get; }

    /// The number of nodes kept in memory.
    int CacheSize { get; set; }

    /// Heap assigned to the StorageEngine instance.
    IHeap Heap { get; }

    void Commit();
    void Close();
}
