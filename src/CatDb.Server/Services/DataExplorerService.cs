// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using CatDb.Data;
using CatDb.Database;

namespace CatDb.Server.Services;

public sealed class DataExplorerService(DatabaseHostService hostService)
{
    private static readonly JsonSerializerOptions SerializerOptions =
        new() { IncludeFields = true };

    public object BrowseTable(
        string databaseName,
        string tableName,
        int take,
        string? fromKey,
        string? toKey,
        string direction)
    {
        var engine = hostService.GetOrOpenDatabase(databaseName);

        if (!engine.Exists(tableName))
            throw new KeyNotFoundException($"Table '{tableName}' not found.");

        var descriptor = engine[tableName];
        if (descriptor.StructureType != StructureType.XTABLE)
            throw new InvalidOperationException($"'{tableName}' is not a table.");

        take = Math.Clamp(take, 1, 1_000);

        var keyDataType    = descriptor.KeyDataType;
        var recordDataType = descriptor.RecordDataType;
        var keyMembers     = descriptor.KeyMembers;
        var recordMembers  = descriptor.RecordMembers;

        var table = engine.OpenXTablePortable(tableName, keyDataType, recordDataType);

        var hasFrom = TryParseKey(fromKey, keyDataType, out var fromData);
        var hasTo   = TryParseKey(toKey,   keyDataType, out var toData);

        var isForward = !string.Equals(direction, "backward", StringComparison.OrdinalIgnoreCase);

        IEnumerable<KeyValuePair<IData, IData>> scan;

        if (isForward)
            scan = (hasFrom || hasTo)
                ? table.Forward(fromData!, hasFrom, toData!, hasTo)
                : table.Forward();
        else
            scan = (hasFrom || hasTo)
                ? table.Backward(toData!, hasTo, fromData!, hasFrom)
                : table.Backward();

        var keyNameMap    = BuildIndexToNameMap(keyMembers);
        var recordNameMap = BuildIndexToNameMap(recordMembers);

        var rows = scan
            .Take(take)
            .Select(kv => new
            {
                key   = ToJsonNode(kv.Key, keyNameMap),
                value = ToJsonNode(kv.Value, recordNameMap),
            })
            .ToList();

        return new
        {
            database    = databaseName,
            table       = tableName,
            keySchema   = DataSchemaHelper.Describe(keyDataType, keyMembers),
            valueSchema = DataSchemaHelper.Describe(recordDataType, recordMembers),
            direction,
            take,
            count       = rows.Count,
            rows,
        };
    }

    private static Dictionary<int, string>? BuildIndexToNameMap(IReadOnlyDictionary<string, int>? members)
    {
        if (members == null) return null;
        var map = new Dictionary<int, string>(members.Count);
        foreach (var kv in members)
            map[kv.Value] = kv.Key;
        return map;
    }

    private static JsonNode? ToJsonNode(IData? data, Dictionary<int, string>? nameMap)
    {
        if (data == null) return null;

        var valueField = data.GetType().GetField("Value");
        if (valueField == null) return null;

        var value = valueField.GetValue(data);
        if (value == null) return null;

        var node = JsonSerializer.SerializeToNode(value, value.GetType(), SerializerOptions);

        if (nameMap != null && node is JsonObject obj)
        {
            var renamed = new JsonObject();
            foreach (var kv in obj)
            {
                if (kv.Key.StartsWith("Slot", StringComparison.Ordinal)
                    && int.TryParse(kv.Key.AsSpan(4), out var idx)
                    && nameMap.TryGetValue(idx, out var realName))
                {
                    renamed[realName] = kv.Value?.DeepClone();
                }
                else
                {
                    renamed[kv.Key] = kv.Value?.DeepClone();
                }
            }
            return renamed;
        }

        return node;
    }

    private static bool TryParseKey(string? keyStr, DataType keyDataType, out IData? key)
    {
        key = null;
        if (keyStr == null || !keyDataType.IsPrimitive) return false;

        var primitiveType = keyDataType.PrimitiveType;
        try
        {
            object parsed;
            if (primitiveType == typeof(string))
                parsed = keyStr;
            else if (primitiveType == typeof(DateTime))
                parsed = DateTime.Parse(keyStr, CultureInfo.InvariantCulture,
                    DateTimeStyles.RoundtripKind);
            else if (primitiveType == typeof(byte[]))
                parsed = Convert.FromBase64String(keyStr);
            else
                parsed = Convert.ChangeType(keyStr, primitiveType,
                    CultureInfo.InvariantCulture);

            var dataType = typeof(Data<>).MakeGenericType(primitiveType);
            key = (IData)Activator.CreateInstance(dataType, parsed)!;
            return true;
        }
        catch
        {
            return false;
        }
    }
}
