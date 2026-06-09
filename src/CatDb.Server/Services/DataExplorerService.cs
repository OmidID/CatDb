// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections;
using System.Globalization;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using CatDb.Data;
using CatDb.Database;
using CatDb.WaterfallTree;

namespace CatDb.Server.Services;

/// <summary>
/// Reads and writes table rows for the HTTP API.  Values are mapped between JSON
/// and the engine's portable <see cref="IData"/> representation <b>recursively</b>,
/// driven by the table's <see cref="DataType"/> and its <see cref="MemberMap"/> so
/// arbitrarily-nested objects/arrays round-trip with their real field names.
/// </summary>
public sealed class DataExplorerService(DatabaseHostService hostService)
{
    private static readonly JsonSerializerOptions SerializerOptions =
        new() { IncludeFields = true };

    // ── Read / browse ─────────────────────────────────────────────────────────

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
        var keyMap         = descriptor.KeyMemberMap;
        var recordMap      = descriptor.RecordMemberMap;

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

        var rows = scan
            .Take(take)
            .Select(kv => new
            {
                key   = DataToJson(kv.Key,   keyDataType,    keyMap),
                value = DataToJson(kv.Value, recordDataType, recordMap),
            })
            .ToList();

        return new
        {
            database    = databaseName,
            table       = tableName,
            keySchema   = TableManagementService.ToJsonSchema(keyDataType, keyMap),
            valueSchema = TableManagementService.ToJsonSchema(recordDataType, recordMap),
            direction,
            take,
            count       = rows.Count,
            rows,
        };
    }

    /// <summary>Unwraps a <c>Data&lt;T&gt;</c> and converts the inner value to JSON.</summary>
    private static JsonNode? DataToJson(IData? data, DataType dataType, MemberMap? map)
    {
        var value = data?.GetType().GetField("Value")?.GetValue(data);
        return ValueToJson(value, dataType, map);
    }

    private static JsonNode? ValueToJson(object? value, DataType dataType, MemberMap? map)
    {
        if (value == null) return null;

        if (dataType.IsPrimitive)
            return JsonSerializer.SerializeToNode(value, value.GetType(), SerializerOptions);

        if (dataType.IsArray || dataType.IsList)
        {
            var arr = new JsonArray();
            var elemType = dataType[0];
            var elemMap  = map?.Element;
            if (value is IEnumerable seq)
                foreach (var item in seq)
                    arr.Add(ValueToJson(item, elemType, elemMap));
            return arr;
        }

        if (dataType.IsSlots)
        {
            var idx2name = map?.NamesByIndex();
            var fields   = OrderedSlotFields(value.GetType());
            var obj      = new JsonObject();
            for (var i = 0; i < dataType.TypesCount && i < fields.Length; i++)
            {
                var name     = idx2name != null && idx2name.TryGetValue(i, out var n) ? n : $"Slot{i}";
                var childMap = map != null && map.Children.TryGetValue(i, out var cm) ? cm : null;
                obj[name] = ValueToJson(fields[i].GetValue(value), dataType[i], childMap);
            }
            return obj;
        }

        // Dictionary / unknown — best-effort direct serialization.
        return JsonSerializer.SerializeToNode(value, value.GetType(), SerializerOptions);
    }

    // ── Write (insert / replace / delete) ──────────────────────────────────────

    public object InsertRecord(string databaseName, string tableName, JsonElement keyJson, JsonElement valueJson)
    {
        var t = OpenTable(databaseName, tableName);
        t.Table.InsertOrIgnore(
            JsonToData(keyJson,   t.KeyDataType,    t.KeyMap),
            JsonToData(valueJson, t.RecordDataType, t.RecordMap));
        return new { success = true, operation = "insert" };
    }

    public object ReplaceRecord(string databaseName, string tableName, JsonElement keyJson, JsonElement valueJson)
    {
        var t = OpenTable(databaseName, tableName);
        t.Table.Replace(
            JsonToData(keyJson,   t.KeyDataType,    t.KeyMap),
            JsonToData(valueJson, t.RecordDataType, t.RecordMap));
        return new { success = true, operation = "replace" };
    }

    public object DeleteRecord(string databaseName, string tableName, JsonElement keyJson)
    {
        var t = OpenTable(databaseName, tableName);
        t.Table.Delete(JsonToData(keyJson, t.KeyDataType, t.KeyMap));
        return new { success = true, operation = "delete" };
    }

    private readonly record struct OpenedTable(
        XTablePortable Table, IStorageEngine Engine,
        DataType KeyDataType, DataType RecordDataType,
        MemberMap? KeyMap, MemberMap? RecordMap);

    private OpenedTable OpenTable(string databaseName, string tableName)
    {
        var engine = hostService.GetOrOpenDatabase(databaseName);
        if (!engine.Exists(tableName))
            throw new KeyNotFoundException($"Table '{tableName}' not found.");
        var descriptor = engine[tableName];
        if (descriptor.StructureType != StructureType.XTABLE)
            throw new InvalidOperationException($"'{tableName}' is not a table.");
        var table = (XTablePortable)engine.OpenXTablePortable(
            tableName, descriptor.KeyDataType, descriptor.RecordDataType);
        return new OpenedTable(table, engine,
            descriptor.KeyDataType, descriptor.RecordDataType,
            descriptor.KeyMemberMap, descriptor.RecordMemberMap);
    }

    /// <summary>Builds a <c>Data&lt;T&gt;</c> from JSON, recursively for the whole DataType tree.</summary>
    private static IData JsonToData(JsonElement json, DataType dataType, MemberMap? map)
    {
        var clrType = DataTypeUtils.BuildType(dataType);
        var value   = JsonToValue(json, dataType, map);
        return (IData)Activator.CreateInstance(typeof(Data<>).MakeGenericType(clrType), value)!;
    }

    private static object? JsonToValue(JsonElement json, DataType dataType, MemberMap? map)
    {
        if (dataType.IsPrimitive)
            return JsonToPrimitive(json, dataType.PrimitiveType);

        if (dataType.IsArray)
        {
            if (json.ValueKind != JsonValueKind.Array)
                throw new ArgumentException("Expected JSON array.");
            var elemType = dataType[0];
            var clrElem  = DataTypeUtils.BuildType(elemType);
            var arr      = Array.CreateInstance(clrElem, json.GetArrayLength());
            var i = 0;
            foreach (var item in json.EnumerateArray())
                arr.SetValue(JsonToValue(item, elemType, map?.Element), i++);
            return arr;
        }

        if (dataType.IsList)
        {
            if (json.ValueKind != JsonValueKind.Array)
                throw new ArgumentException("Expected JSON array.");
            var elemType = dataType[0];
            var clrElem  = DataTypeUtils.BuildType(elemType);
            var list     = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(clrElem))!;
            foreach (var item in json.EnumerateArray())
                list.Add(JsonToValue(item, elemType, map?.Element));
            return list;
        }

        if (dataType.IsSlots)
        {
            if (json.ValueKind != JsonValueKind.Object)
                throw new ArgumentException("Expected JSON object for nested object field.");
            if (map == null)
                throw new InvalidOperationException("Member map missing for object field.");

            var slotsType = DataTypeUtils.BuildType(dataType);
            var instance  = Activator.CreateInstance(slotsType)!;
            var fields    = OrderedSlotFields(slotsType);

            foreach (var prop in json.EnumerateObject())
            {
                if (!map.Names.TryGetValue(prop.Name, out var idx) || idx >= fields.Length)
                    continue;
                var childMap = map.Children.TryGetValue(idx, out var cm) ? cm : null;
                fields[idx].SetValue(instance, JsonToValue(prop.Value, dataType[idx], childMap));
            }
            return instance;
        }

        throw new NotSupportedException($"DataType '{dataType}' not supported for write operations.");
    }

    private static object JsonToPrimitive(JsonElement el, Type targetType)
    {
        if (targetType == typeof(string))   return el.GetString() ?? string.Empty;
        if (targetType == typeof(long))     return el.GetInt64();
        if (targetType == typeof(int))      return el.GetInt32();
        if (targetType == typeof(short))    return el.GetInt16();
        if (targetType == typeof(byte))     return el.GetByte();
        if (targetType == typeof(double))   return el.GetDouble();
        if (targetType == typeof(float))    return el.GetSingle();
        if (targetType == typeof(decimal))  return el.GetDecimal();
        if (targetType == typeof(bool))     return el.GetBoolean();
        if (targetType == typeof(DateTime)) return el.GetDateTime();
        if (targetType == typeof(byte[]))   return Convert.FromBase64String(el.GetString() ?? string.Empty);
        throw new NotSupportedException($"Cannot deserialize JSON to '{targetType.Name}'.");
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns a Slots type's <c>Slot0..N</c> fields ordered by their numeric index
    /// (reflection field order is not guaranteed, so sort defensively).
    /// </summary>
    private static FieldInfo[] OrderedSlotFields(Type slotsType)
    {
        return slotsType.GetFields()
            .Where(f => f.Name.StartsWith("Slot", StringComparison.Ordinal))
            .OrderBy(f => int.TryParse(f.Name.AsSpan(4), out var n) ? n : int.MaxValue)
            .ToArray();
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
