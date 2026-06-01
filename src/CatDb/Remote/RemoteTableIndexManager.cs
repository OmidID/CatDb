// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;
using CatDb.Database.Indexing;
using CatDb.Remote.Commands;

namespace CatDb.Remote;

/// <summary>
/// Client-side index manager that sends index commands to the server.
/// The server's <see cref="XTablePortable"/> handles actual index maintenance.
/// </summary>
internal sealed class RemoteTableIndexManager : ITableIndexManager
{
    private readonly XTableRemote _table;
    private readonly StorageEngineClient _client;
    private readonly List<IndexDefinition> _localCache = new();

    internal RemoteTableIndexManager(XTableRemote table, StorageEngineClient client)
    {
        _table = table;
        _client = client;
    }

    public bool HasIndexes => _localCache.Count > 0;

    public IndexDefinition CreateIndex(string indexName, int[] slotIndices, IndexType type)
    {
        var memberNames = slotIndices.Select(i => $"Slot{i}").ToArray();
        var cmd = new IndexCreateCommand(indexName, slotIndices, memberNames, type);
        _table.Execute(cmd);
        var def = new IndexDefinition(indexName, slotIndices, memberNames, type);
        _localCache.Add(def);
        return def;
    }

    public IndexDefinition CreateIndex(string indexName, string[] memberNames, IndexType type)
    {
        // Server will resolve member names to slot indices
        var cmd = new IndexCreateCommand(indexName, [], memberNames, type);
        _table.Execute(cmd);
        var def = new IndexDefinition(indexName, [], memberNames, type);
        _localCache.Add(def);
        return def;
    }

    public void DropIndex(string indexName)
    {
        var cmd = new IndexDropCommand(indexName);
        _table.Execute(cmd);
        _localCache.RemoveAll(d => d.Name == indexName);
    }

    public IndexDefinition? GetIndex(string indexName)
    {
        return _localCache.Find(d => d.Name == indexName);
    }

    public IReadOnlyList<IndexDefinition> ListIndexes()
    {
        var cmd = new IndexListCommand();
        _table.Execute(cmd);
        if (cmd.Results != null)
        {
            _localCache.Clear();
            _localCache.AddRange(cmd.Results);
        }
        return _localCache.AsReadOnly();
    }

    public void RebuildIndex(string indexName)
    {
        _table.Execute(new IndexRebuildCommand(indexName));
    }

    public void RebuildAllIndexes()
    {
        _table.Execute(new IndexRebuildCommand(null));
    }

    public IEnumerable<KeyValuePair<IData, IData>> FindByIndex(string indexName, IData fieldValue)
    {
        var cmd = new IndexFindCommand(indexName, fieldValue);
        _table.Execute(cmd);
        return cmd.Results ?? [];
    }

    public IEnumerable<IData> FindKeysByIndex(string indexName, IData fieldValue)
    {
        var cmd = new IndexFindCommand(indexName, fieldValue);
        _table.Execute(cmd);
        return cmd.Results?.Select(kv => kv.Key) ?? [];
    }

    public IEnumerable<KeyValuePair<IData, IData>> FindByIndexRange(
        string indexName, IData? from, bool hasFrom, IData? to, bool hasTo)
    {
        var cmd = new IndexFindRangeCommand(indexName, from, hasFrom, to, hasTo);
        _table.Execute(cmd);
        return cmd.Results ?? [];
    }

    public bool ExistsInIndex(string indexName, IData fieldValue)
    {
        var cmd = new IndexExistsCommand(indexName, fieldValue);
        _table.Execute(cmd);
        return cmd.Result;
    }

    public long CountByIndex(string indexName, IData fieldValue)
    {
        var cmd = new IndexCountCommand(indexName, fieldValue);
        _table.Execute(cmd);
        return cmd.Result;
    }
}
