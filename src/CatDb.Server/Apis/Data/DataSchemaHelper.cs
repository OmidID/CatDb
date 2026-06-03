// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;

namespace CatDb.Server.Services;

internal static class DataSchemaHelper
{
    public static object Describe(DataType dt, IReadOnlyDictionary<string, int>? members = null)
    {
        if (dt.IsPrimitive)
            return new { kind = dt.ToString() };

        if (dt.IsSlots)
        {
            // Build index → name lookup from the persisted members map.
            Dictionary<int, string>? indexToName = null;
            if (members != null)
            {
                indexToName = new Dictionary<int, string>(members.Count);
                foreach (var kv in members)
                    indexToName[kv.Value] = kv.Key;
            }

            return new
            {
                kind   = "Slots",
                fields = Enumerable.Range(0, dt.TypesCount)
                    .Select(i => new
                    {
                        index  = i,
                        name   = indexToName != null && indexToName.TryGetValue(i, out var n) ? n : $"Slot{i}",
                        schema = Describe(dt[i]),
                    })
                    .ToArray(),
            };
        }

        if (dt.IsArray)
            return new { kind = "Array",  elementType = Describe(dt[0]) };

        if (dt.IsList)
            return new { kind = "List",   elementType = Describe(dt[0]) };

        if (dt.IsDictionary)
            return new
            {
                kind      = "Dictionary",
                keyType   = Describe(dt[0]),
                valueType = Describe(dt[1]),
            };

        // Fallback — should not happen with current DataType values.
        return new { kind = dt.ToString() };
    }
}
