// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Linq.Expressions;
using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public partial class StorageEngine
{
    /// <summary>
    /// Automatic record-schema migration: called from <see cref="Obtain"/> (under the engine lock)
    /// when a table is reopened with a record DataType different from the stored one.
    ///
    /// Strategy — rewrite into a fresh locator (same name, new id), like a SQL table rewrite:
    ///  1. Build a slot mapping old→new. Name-based when both sides have member names (the locator
    ///     persists name→slot maps; the new names come from the caller's record type / member map):
    ///     same name ⇒ copy (slot DataType must match), new name ⇒ default value, removed ⇒ dropped.
    ///     Without names, positional-prefix: slots are matched by position; appended slots get
    ///     defaults, truncated slots are dropped.
    ///  2. Read every row with the OLD layout, transform, buffer.
    ///  3. Clear + soft-delete the old locator (and this table's now-stale secondary index tables).
    ///  4. Create the new locator under the same name and reinsert the transformed rows.
    ///
    /// Not migratable (throws a diagnostic ArgumentException): non-Slots (primitive) record schemas,
    /// and a same-named member whose slot DataType changed.
    ///
    /// Note: rows are materialized in memory during the rewrite (v1) — fine for the entity-table
    /// sizes this targets; a streaming rewrite through a temp locator is the follow-up for huge tables.
    /// Secondary indexes are NOT re-created here: their definitions live with the caller
    /// (CreateIndex/EnsureIndex on next use), and CreateIndex backfills from existing rows.
    /// </summary>
    private Item1 MigrateRecordSchema(
        string name, Item1 item, DataType newRecordDataType, Type? newRecordType,
        IReadOnlyDictionary<string, int>? newRecordNames)
    {
        var oldLocator = item.Locator;
        var oldDt      = oldLocator.RecordDataType;

        if (!oldDt.IsSlots || !newRecordDataType.IsSlots)
            throw new ArgumentException(
                $"Record schema mismatch for table '{name}': stored record schema is {oldDt}, " +
                $"but it was opened with {newRecordDataType}. Only composite (multi-field) record schemas " +
                "can be migrated automatically; a primitive record type cannot change. " +
                "Open with the original type, or delete/recreate the table.");

        // Resolve CLR types. The old CLR class may no longer exist (renamed/reshaped entity) —
        // fall back to the portable Slots type; its persist layout is identical.
        var oldRecordType = oldLocator.RecordType ?? DataTypeUtils.BuildType(oldDt);
        newRecordType   ??= DataTypeUtils.BuildType(newRecordDataType);

        // Member names: old side from the persisted locator map; new side from the caller
        // (typed open → record type's public members; portable open → provided member map).
        var oldNames = oldLocator.RecordMembers;
        newRecordNames ??= SchemaMigrator.NamesFromType(newRecordType);

        var transform = SchemaMigrator.BuildRowTransformer(
            name, oldRecordType, oldDt, oldNames, newRecordType, newRecordDataType, newRecordNames);

        // 2. Read all rows in the old layout.
        if (oldLocator.RecordType is null)
            oldLocator.RecordType = oldRecordType;
        if (!oldLocator.IsReady)
            oldLocator.Prepare();
        item.Table ??= new XTablePortable(this, oldLocator);
        item.Table.Flush();

        var rows = new List<KeyValuePair<IData, IData>>();
        foreach (var kv in item.Table.Forward())
            rows.Add(new KeyValuePair<IData, IData>(kv.Key, transform(kv.Value)));

        // 3. Retire the old locator + this table's secondary index tables (their composite keys
        //    embed record slots by position — stale after the migration; recreated+backfilled by
        //    the next CreateIndex).
        item.Table.Clear();
        item.Table.Flush();
        oldLocator.IsDeleted = true;
        _map.Remove(name);

        var idxPrefix = $"{InternalNaming.ReservedPrefix}idx_{name}_";
        foreach (var idxName in _map.Keys.Where(k => k.StartsWith(idxPrefix, StringComparison.Ordinal)).ToList())
            RemoveInternalTable(idxName);

        // 4. New locator, same name; carry over the key member map (key schema is unchanged).
        // Record map: only from a typed record class here. For portable (Slots) records the map is
        // left NULL on purpose — the open overload calls SetMembers with the client's full recursive
        // map right after Obtain (SetMembers is only-if-null, so setting a flat map here would block
        // it; and MemberMap.Build on a Slots type would persist useless "Slot0"/"Slot1" names).
        MemberMap? newRecordMap = null;
        if (!DataTypeUtils.IsAnonymousType(newRecordType) && !typeof(ISlots).IsAssignableFrom(newRecordType))
            newRecordMap = MemberMap.Build(newRecordDataType, newRecordType);

        var newLocator = CreateLocator(name, oldLocator.StructureType,
            oldLocator.KeyDataType, newRecordDataType, oldLocator.KeyType, newRecordType);
        newLocator.SetMembers(oldLocator.KeyMemberMap, newRecordMap);
        if (!newLocator.IsReady)
            newLocator.Prepare();

        var newItem = new Item1(newLocator, new XTablePortable(this, newLocator));
        _map[name] = newItem;

        foreach (var kv in rows)
            newItem.Table.Replace(kv.Key, kv.Value);
        newItem.Table.Flush();

        return newItem;
    }

    /// <summary>
    /// Opens (creating if absent) an engine-internal backing table, REUSING the persisted locator
    /// when its schema still matches — this is what makes secondary-index data survive a process
    /// restart. On a schema mismatch (main table migrated ⇒ the index composite key changed) the
    /// stale table is dropped and a fresh one is created. Routing internal tables through
    /// <see cref="Obtain"/> also enrolls them in <c>_map</c>, so their buffered operations are
    /// flushed by <see cref="Commit"/> like any public table.
    /// </summary>
    internal XTablePortable ObtainInternalTable(
        string name, DataType keyDataType, DataType recordDataType, Type keyType, Type recordType,
        out bool preexisting)
    {
        if (!InternalNaming.IsInternal(name))
            throw new ArgumentException(
                $"Internal table name must start with '{InternalNaming.ReservedPrefix}'.", nameof(name));

        _syncRoot.Enter();
        try
        {
            if (_map.TryGetValue(name, out var existing))
            {
                if (existing.Locator.KeyDataType == keyDataType &&
                    existing.Locator.RecordDataType == recordDataType)
                {
                    preexisting = true;
                    existing.Locator.KeyType    ??= keyType;
                    existing.Locator.RecordType ??= recordType;
                    if (!existing.Locator.IsReady)
                        existing.Locator.Prepare();
                    existing.Table ??= new XTablePortable(this, existing.Locator);
                    return existing.Table;
                }

                // Stale layout — drop and recreate below.
                RemoveInternalTable(name);
            }

            preexisting = false;
            var item = Obtain(name, StructureType.XTABLE, keyDataType, recordDataType, keyType, recordType, allowInternal: true);
            return item.Table;
        }
        finally { _syncRoot.Exit(); }
    }

    /// <summary>Clears + soft-deletes an engine-internal table and forgets it. No-op when absent.</summary>
    internal void RemoveInternalTable(string name)
    {
        _syncRoot.Enter();
        try
        {
            if (!_map.TryGetValue(name, out var item))
                return;

            if (!item.Locator.IsReady)
                item.Locator.Prepare();
            item.Table ??= new XTablePortable(this, item.Locator);
            item.Table.Clear();
            item.Table.Flush();
            item.Locator.IsDeleted = true;
            _map.Remove(name);
        }
        finally { _syncRoot.Exit(); }
    }
}

/// <summary>
/// Builds the old→new record transformation for an automatic schema migration
/// (see <see cref="StorageEngine.MigrateRecordSchema"/>).
/// </summary>
internal static class SchemaMigrator
{
    /// <summary>Top-level member name → slot index for a record CLR type (null for anonymous/Slots types).</summary>
    internal static IReadOnlyDictionary<string, int>? NamesFromType(Type recordType)
    {
        if (DataTypeUtils.IsAnonymousType(recordType) || typeof(ISlots).IsAssignableFrom(recordType))
            return null;

        var names = new Dictionary<string, int>(StringComparer.Ordinal);
        var i = 0;
        foreach (var member in DataTypeUtils.GetPublicMembers(recordType))
            names[member.Name] = i++;
        return names.Count > 0 ? names : null;
    }

    /// <summary>
    /// Compiles a row transformer old-record → new-record according to the slot mapping.
    /// Mapping is name-based when both name sets are known, else positional-prefix.
    /// Throws a diagnostic ArgumentException for non-migratable changes.
    /// </summary>
    internal static Func<IData, IData> BuildRowTransformer(
        string tableName,
        Type oldRecordType, DataType oldDt, IReadOnlyDictionary<string, int>? oldNames,
        Type newRecordType, DataType newDt, IReadOnlyDictionary<string, int>? newNames)
    {
        var mapping = BuildSlotMapping(tableName, oldDt, oldNames, newDt, newNames);

        var input  = Expression.Parameter(typeof(object), "oldRecord");
        var oldVar = Expression.Variable(oldRecordType, "old");
        var newVar = Expression.Variable(newRecordType, "new");

        var body = new List<Expression>
        {
            Expression.Assign(oldVar, Expression.Convert(input, oldRecordType)),
        };

        if (typeof(ISlots).IsAssignableFrom(newRecordType))
        {
            // Portable record: single ctor taking all slots in order.
            var ctor = newRecordType.GetConstructors()
                .OrderByDescending(c => c.GetParameters().Length).First();
            var ps   = ctor.GetParameters();
            var args = new Expression[ps.Length];
            for (var i = 0; i < ps.Length; i++)
                args[i] = SlotValue(oldVar, oldRecordType, mapping[i], ps[i].ParameterType);
            body.Add(Expression.Assign(newVar, Expression.New(ctor, args)));
        }
        else
        {
            // POCO record: parameterless ctor + member assignments.
            body.Add(Expression.Assign(newVar, Expression.New(newRecordType)));
            var members = DataTypeUtils.GetPublicMembers(newRecordType).ToArray();
            for (var i = 0; i < members.Length; i++)
            {
                var target = Expression.PropertyOrField(newVar, members[i].Name);
                body.Add(Expression.Assign(target, SlotValue(oldVar, oldRecordType, mapping[i], target.Type)));
            }
        }

        body.Add(Expression.Convert(newVar, typeof(object)));
        var block = Expression.Block(typeof(object), new[] { oldVar, newVar }, body);
        return Expression.Lambda<Func<IData, IData>>(block, input).Compile();
    }

    /// <summary>Per NEW slot: the OLD slot index that feeds it, or null → default value.</summary>
    private static int?[] BuildSlotMapping(
        string tableName,
        DataType oldDt, IReadOnlyDictionary<string, int>? oldNames,
        DataType newDt, IReadOnlyDictionary<string, int>? newNames)
    {
        var oldCount = oldDt.TypesCount;
        var newCount = newDt.TypesCount;
        var mapping  = new int?[newCount];

        if (oldNames is { Count: > 0 } && newNames is { Count: > 0 })
        {
            var newByIndex = newNames.ToDictionary(kv => kv.Value, kv => kv.Key);
            for (var i = 0; i < newCount; i++)
            {
                if (!newByIndex.TryGetValue(i, out var memberName) ||
                    !oldNames.TryGetValue(memberName, out var oldSlot))
                {
                    mapping[i] = null; // new member → default value
                    continue;
                }

                if (oldDt[oldSlot] != newDt[i])
                    throw new ArgumentException(
                        $"Cannot migrate table '{tableName}': member '{memberName}' changed type " +
                        $"from {oldDt[oldSlot]} to {newDt[i]}. Changing an existing member's type is " +
                        "not supported — rename the new member (old data is dropped) or migrate manually.");

                mapping[i] = oldSlot;
            }
            return mapping;
        }

        // No member names (untyped portable open) — positional prefix match.
        var shared = Math.Min(oldCount, newCount);
        for (var i = 0; i < shared; i++)
        {
            if (oldDt[i] != newDt[i])
                throw new ArgumentException(
                    $"Cannot migrate table '{tableName}': no member names are stored for the old schema and " +
                    $"the layouts diverge at slot {i} ({oldDt[i]} vs {newDt[i]}). Positional migration only " +
                    "supports appending new fields at the end or truncating trailing fields. Reopen the " +
                    "table with a typed record class (so member names persist), or migrate manually.");
            mapping[i] = i;
        }
        for (var i = shared; i < newCount; i++)
            mapping[i] = null;
        return mapping;
    }

    /// <summary>Expression producing the NEW slot value: mapped old slot (converted if the CLR
    /// representation differs, e.g. POCO Guid ↔ portable byte[]) or the type's default.</summary>
    private static Expression SlotValue(Expression oldVar, Type oldRecordType, int? oldSlot, Type targetType)
    {
        if (oldSlot is null)
            return Expression.Default(targetType);

        var source = SlotAccess(oldVar, oldRecordType, oldSlot.Value);
        if (source.Type == targetType)
            return source;

        // Same slot DataType, different CLR representation (enum↔integral, Guid↔byte[], POCO↔Slots
        // nesting) — route through the transformer body, which handles all conversion pairs.
        var slotVar = Expression.Variable(targetType);
        return Expression.Block(targetType, new[] { slotVar },
            TransformerHelper.BuildBody(slotVar, source, null, null),
            slotVar);
    }

    private static Expression SlotAccess(Expression instance, Type recordType, int slot)
    {
        var slotField = recordType.GetField($"Slot{slot}");
        if (slotField != null)
            return Expression.Field(instance, slotField);

        var member = DataTypeUtils.GetPublicMembers(recordType).ElementAt(slot);
        return Expression.PropertyOrField(instance, member.Name);
    }
}
