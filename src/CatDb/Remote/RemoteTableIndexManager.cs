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
        var cmd = new IndexFindCommand(indexName, RemoteFieldCodec.Serialize(fieldValue));
        _table.Execute(cmd);
        return cmd.Results ?? [];
    }

    public IEnumerable<IData> FindKeysByIndex(string indexName, IData fieldValue)
    {
        var cmd = new IndexFindCommand(indexName, RemoteFieldCodec.Serialize(fieldValue));
        _table.Execute(cmd);
        return cmd.Results?.Select(kv => kv.Key) ?? [];
    }

    public IEnumerable<KeyValuePair<IData, IData>> FindByIndexRange(
        string indexName,
        IData? from, bool hasFrom, bool fromInclusive,
        IData? to, bool hasTo, bool toInclusive,
        bool backward)
    {
        var cmd = new IndexFindRangeCommand(
            indexName,
            hasFrom ? RemoteFieldCodec.Serialize(from!) : null, hasFrom, fromInclusive,
            hasTo ? RemoteFieldCodec.Serialize(to!) : null, hasTo, toInclusive,
            backward);
        _table.Execute(cmd);
        return cmd.Results ?? [];
    }

    public IEnumerable<KeyValuePair<IData, IData>> FindByIndexPrefix(
        string indexName, IData prefixValue, int prefixFieldCount, bool backward)
    {
        var cmd = new IndexFindPrefixCommand(
            indexName, RemoteFieldCodec.Serialize(prefixValue), prefixFieldCount, backward);
        _table.Execute(cmd);
        return cmd.Results ?? [];
    }

    public bool ExistsInIndex(string indexName, IData fieldValue)
    {
        var cmd = new IndexExistsCommand(indexName, RemoteFieldCodec.Serialize(fieldValue));
        _table.Execute(cmd);
        return cmd.Result;
    }

    public long CountByIndex(string indexName, IData fieldValue)
    {
        var cmd = new IndexCountCommand(indexName, RemoteFieldCodec.Serialize(fieldValue));
        _table.Execute(cmd);
        return cmd.Result;
    }

    public IEnumerable<KeyValuePair<IData, IData>> ExecuteQuery(CatDb.Database.Querying.EngineQuery query)
    {
        var filterRoot = ToWireNode(query.Filter);

        var sorts = query.Sorts
            .Select(s => new WireSort { Member = s.Member, Descending = s.Descending })
            .ToList();

        var keyType = _table.Descriptor.KeyType
                      ?? CatDb.Data.DataTypeUtils.BuildType(_table.Descriptor.KeyDataType);

        var cmd = new IndexQueryCommand(filterRoot, sorts)
        {
            HasKeyFrom = query.HasKeyFrom,
            KeyFromInclusive = query.KeyFromInclusive,
            KeyFromRaw = query.KeyFrom != null ? RemoteFieldCodec.Serialize(query.KeyFrom, keyType) : null,
            HasKeyTo = query.HasKeyTo,
            KeyToInclusive = query.KeyToInclusive,
            KeyToRaw = query.KeyTo != null ? RemoteFieldCodec.Serialize(query.KeyTo, keyType) : null,
            Skip = query.Skip,
            HasTake = query.Take.HasValue,
            Take = query.Take ?? 0,
        };

        _table.Execute(cmd);
        return cmd.Results ?? [];
    }

    /// <summary>
    /// Server-side fast count (index-key-only, no per-row heap fetch) for the same query shape
    /// <see cref="ExecuteQuery"/> runs — a single round trip returning only a <c>long</c>, never
    /// materializing/transferring matched rows. Without this, <c>Query(...).Count()</c> had no remote
    /// fast-count dispatch and fell back to enumerating every matching row over the wire just to discard it.
    /// </summary>
    public long Count(CatDb.Database.Querying.EngineQuery query)
    {
        var filterRoot = ToWireNode(query.Filter);

        var sorts = query.Sorts
            .Select(s => new WireSort { Member = s.Member, Descending = s.Descending })
            .ToList();

        var keyType = _table.Descriptor.KeyType
                      ?? CatDb.Data.DataTypeUtils.BuildType(_table.Descriptor.KeyDataType);

        var cmd = new IndexCountQueryCommand(filterRoot, sorts)
        {
            HasKeyFrom = query.HasKeyFrom,
            KeyFromInclusive = query.KeyFromInclusive,
            KeyFromRaw = query.KeyFrom != null ? RemoteFieldCodec.Serialize(query.KeyFrom, keyType) : null,
            HasKeyTo = query.HasKeyTo,
            KeyToInclusive = query.KeyToInclusive,
            KeyToRaw = query.KeyTo != null ? RemoteFieldCodec.Serialize(query.KeyTo, keyType) : null,
            Skip = query.Skip,
            HasTake = query.Take.HasValue,
            Take = query.Take ?? 0,
        };

        _table.Execute(cmd);
        return cmd.Result;
    }

    private static WireNode? ToWireNode(CatDb.Database.Querying.FilterNode? node)
    {
        switch (node)
        {
            case null:
                return null;
            case CatDb.Database.Querying.PredicateNode p:
                var f = p.Filter;
                return new WireNode
                {
                    Kind = 0,
                    Member = f.Member,
                    Op = (byte)f.Op,
                    FromInclusive = f.FromInclusive,
                    ToInclusive = f.ToInclusive,
                    ValueRaw = f.Value != null ? RemoteFieldCodec.Serialize(f.Value, f.FieldType) : null,
                    Value2Raw = f.Value2 != null ? RemoteFieldCodec.Serialize(f.Value2, f.FieldType) : null,
                };
            case CatDb.Database.Querying.AndNode a:
                return new WireNode { Kind = 1, Children = a.Children.Select(ToWireNode).Where(x => x != null).ToList()! };
            case CatDb.Database.Querying.OrNode o:
                return new WireNode { Kind = 2, Children = o.Children.Select(ToWireNode).Where(x => x != null).ToList()! };
            case CatDb.Database.Querying.NotNode n:
                return new WireNode { Kind = 3, Child = ToWireNode(n.Child) };
            default:
                throw new NotSupportedException($"Unknown filter node {node.GetType().Name}.");
        }
    }
}
