// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Text.Json;
using CatDb.Data;
using CatDb.Database;
using CatDb.Database.Indexing;
using CatDb.WaterfallTree;

namespace CatDb.Server.Services;

/// <summary>
/// Creates, deletes, and introspects tables + indexes via the storage engine.
/// Table schemas are expressed as JSON Schema objects (draft-07 subset).
/// </summary>
public sealed class TableManagementService(DatabaseHostService host)
{
    // ── JSON Schema → DataType ────────────────────────────────────────────────
    //
    // Supported subset:
    //   Primitives  – "type": "string" | "integer" | "number" | "boolean"
    //   Format hint – "format": "int32"|"int64"|"float"|"double"|"datetime"|"decimal"|"bytes"
    //   Object      – "type": "object", "properties": { "Name": <schema>, … }
    //                 Field order = "required" array first (if present), then remaining properties
    //                 in JSON-insertion order.
    //
    // Returns (dataType, memberMap) where memberMap is non-null only for object schemas.

    public static (DataType DataType, MemberMap? Members) ParseJsonSchema(JsonElement schema)
    {
        if (!schema.TryGetProperty("type", out var typeProp))
            throw new ArgumentException("JSON Schema must have a 'type' property.");

        var type   = typeProp.GetString() ?? string.Empty;
        var format = schema.TryGetProperty("format", out var fp) ? fp.GetString() ?? string.Empty : string.Empty;

        switch (type.ToLowerInvariant())
        {
            case "string":
                return format.ToLowerInvariant() switch
                {
                    "date-time" or "datetime" => (DataType.DateTime, null),
                    "byte" or "binary" or "bytes" => (DataType.ByteArray, null),
                    _ => (DataType.String, null),
                };

            case "integer":
                return format.ToLowerInvariant() switch
                {
                    "int32" => (DataType.Int32, null),
                    _       => (DataType.Int64, null),    // default: 64-bit
                };

            case "number":
                return format.ToLowerInvariant() switch
                {
                    "float" or "single" => (DataType.Single, null),
                    "decimal"           => (DataType.Decimal, null),
                    _                   => (DataType.Double, null),  // default: double
                };

            case "boolean":
                return (DataType.Boolean, null);

            case "object":
                return ParseObjectSchema(schema);

            case "array":
                return ParseArraySchema(schema);

            default:
                throw new ArgumentException(
                    $"Unsupported JSON Schema type '{type}'. " +
                    "Use: string, integer, number, boolean, object, array.");
        }
    }

    private static (DataType, MemberMap?) ParseObjectSchema(JsonElement schema)
    {
        if (!schema.TryGetProperty("properties", out var propsEl) ||
            propsEl.ValueKind != JsonValueKind.Object)
            throw new ArgumentException(
                "JSON Schema with type 'object' must have a 'properties' object.");

        // Build ordered field list: required[] first, then remaining in insertion order.
        var order = new List<string>();
        if (schema.TryGetProperty("required", out var reqEl) && reqEl.ValueKind == JsonValueKind.Array)
            foreach (var r in reqEl.EnumerateArray())
                if (r.GetString() is { } n) order.Add(n);

        foreach (var prop in propsEl.EnumerateObject())
            if (!order.Contains(prop.Name, StringComparer.Ordinal))
                order.Add(prop.Name);

        if (order.Count == 0)
            throw new ArgumentException("Object schema 'properties' must have at least one field.");

        var dataTypes = new DataType[order.Count];
        var names     = new Dictionary<string, int>(StringComparer.Ordinal);
        var children  = new Dictionary<int, MemberMap>();

        for (var i = 0; i < order.Count; i++)
        {
            var name = order[i];
            if (!propsEl.TryGetProperty(name, out var fieldSchema))
                throw new ArgumentException($"Field '{name}' listed in 'required' is missing from 'properties'.");

            // Recurse — nested objects become nested Slots, arrays become typed Arrays.
            var (ft, childMap) = ParseJsonSchema(fieldSchema);
            dataTypes[i] = ft;
            names[name]  = i;
            if (childMap != null)
                children[i] = childMap;
        }

        return (DataType.Slots(dataTypes), new MemberMap(names, children.Count > 0 ? children : null));
    }

    private static (DataType, MemberMap?) ParseArraySchema(JsonElement schema)
    {
        if (!schema.TryGetProperty("items", out var itemsEl) ||
            itemsEl.ValueKind != JsonValueKind.Object)
            throw new ArgumentException(
                "JSON Schema with type 'array' must have an 'items' schema object.");

        var (elemType, elemMap) = ParseJsonSchema(itemsEl);
        var map = elemMap == null
            ? null
            : new MemberMap(new Dictionary<string, int>(), null, elemMap);
        return (DataType.Array(elemType), map);
    }

    // ── DataType → JSON Schema (for GET responses) ────────────────────────────

    public static object ToJsonSchema(DataType dt, MemberMap? map = null)
    {
        if (dt.IsPrimitive)
        {
            var t = dt.PrimitiveType;
            if (t == typeof(string))   return new { type = "string" };
            if (t == typeof(int))      return new { type = "integer", format = "int32" };
            if (t == typeof(long))     return new { type = "integer", format = "int64" };
            if (t == typeof(double))   return new { type = "number",  format = "double" };
            if (t == typeof(float))    return new { type = "number",  format = "float" };
            if (t == typeof(bool))     return new { type = "boolean" };
            if (t == typeof(decimal))  return new { type = "number",  format = "decimal" };
            if (t == typeof(DateTime)) return new { type = "string",  format = "date-time" };
            if (t == typeof(byte[]))   return new { type = "string",  format = "bytes" };
            if (t == typeof(short))    return new { type = "integer", format = "int16" };
            if (t == typeof(byte))     return new { type = "integer", format = "int8" };
            return new { type = dt.ToString() };
        }

        if (dt.IsArray || dt.IsList)
            return new { type = "array", items = ToJsonSchema(dt[0], map?.Element) };

        if (dt.IsSlots)
        {
            var idx2name = map?.NamesByIndex();
            var props = new Dictionary<string, object>();
            var required = new List<string>();
            for (var i = 0; i < dt.TypesCount; i++)
            {
                var name = idx2name != null && idx2name.TryGetValue(i, out var n) ? n : $"Slot{i}";
                var childMap = map != null && map.Children.TryGetValue(i, out var cm) ? cm : null;
                props[name] = ToJsonSchema(dt[i], childMap);
                required.Add(name);
            }
            return new { type = "object", properties = props, required };
        }

        return new { type = dt.ToString() };
    }

    // ── Table lifecycle ───────────────────────────────────────────────────────

    public IReadOnlyList<TableSummary> ListTables(string databaseName)
    {
        var engine = host.GetOrOpenDatabase(databaseName);
        return engine
            .Where(d => d.StructureType == StructureType.XTABLE)
            .Select(d => new TableSummary(
                d.Name ?? string.Empty,
                ToJsonSchema(d.KeyDataType, d.KeyMemberMap),
                ToJsonSchema(d.RecordDataType, d.RecordMemberMap),
                d.CreateTime,
                d.ModifiedTime))
            .ToList();
    }

    public TableInfo CreateTable(
        string databaseName,
        string tableName,
        JsonElement keySchema,
        JsonElement valueSchema)
    {
        var engine = host.GetOrOpenDatabase(databaseName);
        if (engine.Exists(tableName))
            throw new InvalidOperationException($"Table '{tableName}' already exists.");

        var (keyType,    keyMembers)    = ParseJsonSchema(keySchema);
        var (recordType, recordMembers) = ParseJsonSchema(valueSchema);

        var table = (XTablePortable)engine.OpenXTablePortable(
            tableName, keyType, recordType, keyMembers, recordMembers);
        table.Flush();
        engine.Commit();

        var desc = engine[tableName];
        return DescribeTable(databaseName, desc);
    }

    public void DeleteTable(string databaseName, string tableName)
    {
        var engine = host.GetOrOpenDatabase(databaseName);
        if (!engine.Exists(tableName))
            throw new KeyNotFoundException($"Table '{tableName}' not found.");
        engine.Delete(tableName);
        engine.Commit();
    }

    public TableInfo GetTable(string databaseName, string tableName)
    {
        var engine = host.GetOrOpenDatabase(databaseName);
        if (!engine.Exists(tableName))
            throw new KeyNotFoundException($"Table '{tableName}' not found.");
        var desc = engine[tableName];
        if (desc.StructureType != StructureType.XTABLE)
            throw new InvalidOperationException($"'{tableName}' is not a table.");
        return DescribeTable(databaseName, desc,
            (XTablePortable)engine.OpenXTablePortable(tableName, desc.KeyDataType, desc.RecordDataType));
    }

    // ── Index lifecycle ───────────────────────────────────────────────────────

    public IndexInfo CreateIndex(
        string databaseName,
        string tableName,
        string indexName,
        string[] memberNames,
        IndexType indexType)
    {
        var (table, _) = OpenTable(databaseName, tableName);
        var def = table.Indexes.CreateIndex(indexName, memberNames, indexType);
        table.Flush();
        host.GetOrOpenDatabase(databaseName).Commit();
        return new IndexInfo(def.Name, def.MemberNames, def.SlotIndices, def.Type.ToString());
    }

    public void DropIndex(string databaseName, string tableName, string indexName)
    {
        var (table, engine) = OpenTable(databaseName, tableName);
        table.Indexes.DropIndex(indexName);
        table.Flush();
        engine.Commit();
    }

    public void RebuildIndex(string databaseName, string tableName, string? indexName)
    {
        var (table, _) = OpenTable(databaseName, tableName);
        if (indexName is null)
            table.Indexes.RebuildAllIndexes();
        else
            table.Indexes.RebuildIndex(indexName);
    }

    public IReadOnlyList<IndexInfo> ListIndexes(string databaseName, string tableName)
    {
        var (table, _) = OpenTable(databaseName, tableName);
        return table.Indexes.ListIndexes()
            .Select(d => new IndexInfo(d.Name, d.MemberNames, d.SlotIndices, d.Type.ToString()))
            .ToList();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private (XTablePortable Table, IStorageEngine Engine) OpenTable(string databaseName, string tableName)
    {
        var engine = host.GetOrOpenDatabase(databaseName);
        if (!engine.Exists(tableName))
            throw new KeyNotFoundException($"Table '{tableName}' not found.");
        var desc = engine[tableName];
        if (desc.StructureType != StructureType.XTABLE)
            throw new InvalidOperationException($"'{tableName}' is not a table.");
        return ((XTablePortable)engine.OpenXTablePortable(tableName, desc.KeyDataType, desc.RecordDataType), engine);
    }

    private static TableInfo DescribeTable(string databaseName, IDescriptor desc, XTablePortable? table = null)
    {
        var indexes = table?.Indexes.ListIndexes()
            .Select(d => new IndexInfo(d.Name, d.MemberNames, d.SlotIndices, d.Type.ToString()))
            .ToArray() ?? [];

        return new TableInfo(
            databaseName,
            desc.Name ?? string.Empty,
            ToJsonSchema(desc.KeyDataType, desc.KeyMemberMap),
            ToJsonSchema(desc.RecordDataType, desc.RecordMemberMap),
            desc.CreateTime,
            desc.ModifiedTime,
            indexes);
    }
}

// ── DTOs ──────────────────────────────────────────────────────────────────────

public sealed record CreateTableRequest
{
    public required string Name { get; init; }
    public required JsonElement KeySchema { get; init; }
    public required JsonElement ValueSchema { get; init; }
}

public sealed record CreateIndexRequest
{
    public required string IndexName { get; init; }
    public required string[] Members { get; init; }
    public string Type { get; init; } = "NonUnique";
}

public sealed record TableSummary(
    string Name,
    object KeySchema,
    object ValueSchema,
    DateTime CreatedAt,
    DateTime ModifiedAt);

public sealed record TableInfo(
    string Database,
    string Name,
    object KeySchema,
    object ValueSchema,
    DateTime CreatedAt,
    DateTime ModifiedAt,
    IndexInfo[] Indexes);

public sealed record IndexInfo(
    string Name,
    string[] Members,
    int[] SlotIndices,
    string Type);
