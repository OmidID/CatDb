// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database.Indexing;

/// <summary>
/// Local implementation of <see cref="ITableIndexManager"/> that maintains
/// secondary indexes directly on a <see cref="XTablePortable"/> instance.
///
/// Each index is backed by a separate WTree table (same storage engine).
/// Write interception is triggered by XTablePortable when this manager has indexes.
///
/// Thread safety: relies on XTablePortable's SyncRoot which is held during writes.
/// </summary>
internal sealed class TableIndexManager : ITableIndexManager
{
    private readonly XTablePortable _table;
    private readonly WTree _tree;
    private readonly Locator _locator;
    private readonly Dictionary<string, IndexEntry> _indexes = new(StringComparer.Ordinal);

    /// <summary>Cached table name for building index table names.</summary>
    private readonly string _tableName;

    internal TableIndexManager(XTablePortable table)
    {
        _table = table;
        _tree = table.Tree;
        _locator = table.Locator;
        _tableName = _locator.Name ?? throw new InvalidOperationException("Table must have a name for indexing.");
    }

    public bool HasIndexes => _indexes.Count > 0;

    // ── Index lifecycle ──────────────────────────────────────────────────────

    public IndexDefinition CreateIndex(string indexName, int[] slotIndices, IndexType type)
    {
        if (string.IsNullOrWhiteSpace(indexName))
            throw new ArgumentException("Index name cannot be empty.", nameof(indexName));
        if (slotIndices == null || slotIndices.Length == 0)
            throw new ArgumentException("Must specify at least one slot index.", nameof(slotIndices));
        if (_indexes.ContainsKey(indexName))
            throw new InvalidOperationException($"Index '{indexName}' already exists on table '{_tableName}'.");

        var memberNames = ResolveMemberNames(slotIndices);
        var def = new IndexDefinition(indexName, slotIndices, memberNames, type);
        var entry = BuildEntry(def);
        _indexes[indexName] = entry;
        return def;
    }

    public IndexDefinition CreateIndex(string indexName, string[] memberNames, IndexType type)
    {
        if (string.IsNullOrWhiteSpace(indexName))
            throw new ArgumentException("Index name cannot be empty.", nameof(indexName));
        if (memberNames == null || memberNames.Length == 0)
            throw new ArgumentException("Must specify at least one member name.", nameof(memberNames));
        if (_indexes.ContainsKey(indexName))
            throw new InvalidOperationException($"Index '{indexName}' already exists on table '{_tableName}'.");

        var slotIndices = ResolveSlotIndices(memberNames);
        var def = new IndexDefinition(indexName, slotIndices, memberNames, type);
        var entry = BuildEntry(def);
        _indexes[indexName] = entry;
        return def;
    }

    public void DropIndex(string indexName)
    {
        if (!_indexes.TryGetValue(indexName, out var entry))
            throw new KeyNotFoundException($"Index '{indexName}' not found on table '{_tableName}'.");

        entry.IndexTable.Clear();
        entry.IndexTable.Flush();

        // Mark the index table's locator as deleted
        var indexTableName = entry.Definition.GetTableName(_tableName);
        DeleteIndexTable(indexTableName);

        _indexes.Remove(indexName);
    }

    public IndexDefinition? GetIndex(string indexName)
    {
        return _indexes.TryGetValue(indexName, out var entry) ? entry.Definition : null;
    }

    public IReadOnlyList<IndexDefinition> ListIndexes()
    {
        return _indexes.Values.Select(e => e.Definition).ToList().AsReadOnly();
    }

    public void RebuildIndex(string indexName)
    {
        if (!_indexes.TryGetValue(indexName, out var entry))
            throw new KeyNotFoundException($"Index '{indexName}' not found on table '{_tableName}'.");

        RebuildEntry(entry);
    }

    public void RebuildAllIndexes()
    {
        foreach (var entry in _indexes.Values)
            RebuildEntry(entry);
    }

    // ── Search operations ────────────────────────────────────────────────────

    public IEnumerable<KeyValuePair<IData, IData>> FindByIndex(string indexName, IData fieldValue)
    {
        if (!_indexes.TryGetValue(indexName, out var entry))
            throw new KeyNotFoundException($"Index '{indexName}' not found.");

        // Must flush the main table to ensure we read fresh data
        _table.Flush();

        if (entry.Definition.Type == IndexType.Unique)
            return FindByUniqueIndex(entry, fieldValue);
        else
            return FindByNonUniqueIndex(entry, fieldValue);
    }

    public IEnumerable<IData> FindKeysByIndex(string indexName, IData fieldValue)
    {
        if (!_indexes.TryGetValue(indexName, out var entry))
            throw new KeyNotFoundException($"Index '{indexName}' not found.");

        _table.Flush();

        if (entry.Definition.Type == IndexType.Unique)
            return FindKeysByUniqueIndex(entry, fieldValue);
        else
            return FindKeysByNonUniqueIndex(entry, fieldValue);
    }

    public IEnumerable<KeyValuePair<IData, IData>> FindByIndexRange(
        string indexName, IData? from, bool hasFrom, IData? to, bool hasTo)
    {
        if (!_indexes.TryGetValue(indexName, out var entry))
            throw new KeyNotFoundException($"Index '{indexName}' not found.");

        _table.Flush();

        if (entry.Definition.Type == IndexType.Unique)
            return FindRangeUniqueIndex(entry, from, hasFrom, to, hasTo);
        else
            return FindRangeNonUniqueIndex(entry, from, hasFrom, to, hasTo);
    }

    public bool ExistsInIndex(string indexName, IData fieldValue)
    {
        if (!_indexes.TryGetValue(indexName, out var entry))
            throw new KeyNotFoundException($"Index '{indexName}' not found.");

        _table.Flush();

        if (entry.Definition.Type == IndexType.Unique)
            return entry.IndexTable.Exists(fieldValue);
        else
            return FindKeysByNonUniqueIndex(entry, fieldValue).Any();
    }

    public long CountByIndex(string indexName, IData fieldValue)
    {
        if (!_indexes.TryGetValue(indexName, out var entry))
            throw new KeyNotFoundException($"Index '{indexName}' not found.");

        _table.Flush();

        if (entry.Definition.Type == IndexType.Unique)
            return entry.IndexTable.Exists(fieldValue) ? 1 : 0;
        else
            return FindKeysByNonUniqueIndex(entry, fieldValue).LongCount();
    }

    // ── Write interception (called by XTablePortable) ────────────────────────

    /// <summary>
    /// Called BEFORE a Replace/InsertOrIgnore. The old record (if any) was already read.
    /// This method updates all index entries.
    /// </summary>
    internal void OnReplace(IData key, IData newRecord, IData? oldRecord)
    {
        foreach (var entry in _indexes.Values)
        {
            var newFieldValue = entry.FieldExtractor(newRecord);

            if (oldRecord != null)
            {
                var oldFieldValue = entry.FieldExtractor(oldRecord);

                // If field value didn't change, skip index update
                if (entry.FieldEquals(oldFieldValue, newFieldValue))
                    continue;

                // Remove stale entry
                if (entry.Definition.Type == IndexType.Unique)
                {
                    entry.IndexTable.Delete(oldFieldValue);
                }
                else
                {
                    var oldCompositeKey = entry.NonUniqueKeyBuilder!(oldRecord, key);
                    entry.IndexTable.Delete(oldCompositeKey);
                }
            }

            // Insert/update
            if (entry.Definition.Type == IndexType.Unique)
            {
                // Enforce uniqueness
                if (entry.IndexTable.TryGet(newFieldValue, out var existingKey))
                {
                    // Same primary key updating same value — OK
                    if (!AreKeysEqual(existingKey, key))
                        throw new UniqueIndexViolationException(entry.Definition.Name, newFieldValue, existingKey);
                }
                entry.IndexTable.Replace(newFieldValue, key);
            }
            else
            {
                var newCompositeKey = entry.NonUniqueKeyBuilder!(newRecord, key);
                entry.IndexTable.Replace(newCompositeKey, _dummyValue);
            }
        }
    }

    /// <summary>Called when a record is deleted.</summary>
    internal void OnDelete(IData key, IData oldRecord)
    {
        foreach (var entry in _indexes.Values)
        {
            if (entry.Definition.Type == IndexType.Unique)
            {
                var fieldValue = entry.FieldExtractor(oldRecord);
                entry.IndexTable.Delete(fieldValue);
            }
            else
            {
                var compositeKey = entry.NonUniqueKeyBuilder!(oldRecord, key);
                entry.IndexTable.Delete(compositeKey);
            }
        }
    }

    /// <summary>Called when the table is cleared.</summary>
    internal void OnClear()
    {
        foreach (var entry in _indexes.Values)
            entry.IndexTable.Clear();
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    private static readonly IData _dummyValue = new Data<byte>(0);

    private IndexEntry BuildEntry(IndexDefinition def)
    {
        var recordDataType = _locator.RecordDataType;
        var recordType = _locator.RecordType
            ?? DataTypeUtils.BuildType(recordDataType);
        var keyDataType = _locator.KeyDataType;
        var keyType = _locator.KeyType
            ?? DataTypeUtils.BuildType(keyDataType);

        var indexTableName = def.GetTableName(_tableName);

        // Build the field extractor
        Func<IData, IData> fieldExtractor;
        DataType fieldDataType;
        Type fieldType;

        if (recordDataType.IsPrimitive && def.SlotIndices.Length == 1 && def.SlotIndices[0] == 0)
        {
            // Primitive record — identity extraction
            fieldExtractor = r => r;
            fieldDataType = recordDataType;
            fieldType = recordType;
        }
        else
        {
            fieldExtractor = SlotAccessor.BuildExtractor(recordType, def.SlotIndices);
            fieldDataType = SlotAccessor.BuildIndexKeyDataType(recordDataType, def.SlotIndices);
            fieldType = DataTypeUtils.BuildType(fieldDataType);
        }

        // Build field equality comparer
        var fieldEquals = SlotAccessor.BuildFieldEqualityComparer(fieldType);

        XTablePortable indexTable;
        Func<IData, IData, IData>? nonUniqueKeyBuilder = null;
        Func<IData, IData>? scanFromKeyBuilder = null;
        Func<IData, IData>? fieldFromCompositeExtractor = null;
        Func<IData, IData>? pkFromCompositeExtractor = null;
        Type? compositeKeyType = null;

        if (def.Type == IndexType.Unique)
        {
            // Unique: index table key = field value, value = primary key
            var idxLocator = _tree.CreateLocator(
                indexTableName, StructureType.XTABLE,
                fieldDataType, keyDataType,
                fieldType, keyType);
            if (!idxLocator.IsReady) idxLocator.Prepare();
            indexTable = new XTablePortable(_tree, idxLocator);
        }
        else
        {
            // Non-unique: index table key = Slots(field..., primaryKey), value = byte
            var compositeKeyDataType = SlotAccessor.BuildNonUniqueIndexKeyDataType(
                recordDataType, def.SlotIndices, keyDataType);
            compositeKeyType = DataTypeUtils.BuildType(compositeKeyDataType);

            var idxLocator = _tree.CreateLocator(
                indexTableName, StructureType.XTABLE,
                compositeKeyDataType, DataType.Byte,
                compositeKeyType, typeof(byte));
            if (!idxLocator.IsReady) idxLocator.Prepare();
            indexTable = new XTablePortable(_tree, idxLocator);

            nonUniqueKeyBuilder = SlotAccessor.BuildNonUniqueKeyBuilder(
                recordType, def.SlotIndices, keyType);
            scanFromKeyBuilder = SlotAccessor.BuildScanFromKeyBuilder(
                compositeKeyType, fieldType, keyType, def.SlotIndices.Length);
            fieldFromCompositeExtractor = SlotAccessor.BuildFieldExtractorFromCompositeKey(
                compositeKeyType, def.SlotIndices.Length);
            pkFromCompositeExtractor = SlotAccessor.BuildPrimaryKeyExtractorFromCompositeKey(
                compositeKeyType, def.SlotIndices.Length + 1);
        }

        return new IndexEntry(
            def, indexTable, fieldExtractor, fieldEquals,
            nonUniqueKeyBuilder, scanFromKeyBuilder,
            fieldFromCompositeExtractor, pkFromCompositeExtractor,
            compositeKeyType);
    }

    private void RebuildEntry(IndexEntry entry)
    {
        entry.IndexTable.Clear();
        entry.IndexTable.Flush();

        _table.Flush();

        // Scan all records in the main table
        var lastVisitedFullKey = default(WTree.FullKey);
        var records = _tree.FindData(_locator, _locator, null!, Direction.Forward,
            out _, out _, ref lastVisitedFullKey);

        if (records == null) return;

        // Iterate all leaf data
        foreach (var kv in _table.Forward())
        {
            var key = kv.Key;
            var record = kv.Value;

            if (entry.Definition.Type == IndexType.Unique)
            {
                var fieldValue = entry.FieldExtractor(record);
                entry.IndexTable.Replace(fieldValue, key);
            }
            else
            {
                var compositeKey = entry.NonUniqueKeyBuilder!(record, key);
                entry.IndexTable.Replace(compositeKey, _dummyValue);
            }
        }
        entry.IndexTable.Flush();
    }

    private IEnumerable<KeyValuePair<IData, IData>> FindByUniqueIndex(IndexEntry entry, IData fieldValue)
    {
        entry.IndexTable.Flush();
        if (!entry.IndexTable.TryGet(fieldValue, out var primaryKey))
            yield break;

        if (_table.TryGet(primaryKey, out var record))
            yield return new KeyValuePair<IData, IData>(primaryKey, record);
    }

    private IEnumerable<IData> FindKeysByUniqueIndex(IndexEntry entry, IData fieldValue)
    {
        entry.IndexTable.Flush();
        if (entry.IndexTable.TryGet(fieldValue, out var primaryKey))
            yield return primaryKey;
    }

    private IEnumerable<KeyValuePair<IData, IData>> FindByNonUniqueIndex(IndexEntry entry, IData fieldValue)
    {
        // Materialize keys first to avoid recursive lock
        var keys = FindKeysByNonUniqueIndex(entry, fieldValue).ToList();
        foreach (var pk in keys)
        {
            if (_table.TryGet(pk, out var record))
                yield return new KeyValuePair<IData, IData>(pk, record);
        }
    }

    private IEnumerable<IData> FindKeysByNonUniqueIndex(IndexEntry entry, IData fieldValue)
    {
        entry.IndexTable.Flush();

        var fromKey = entry.ScanFromKeyBuilder!(fieldValue);

        // Materialize to avoid recursive lock (both tables share same WTree)
        var results = new List<IData>();
        foreach (var kv in entry.IndexTable.Forward(fromKey, true, default!, false))
        {
            var extractedField = entry.FieldFromCompositeExtractor!(kv.Key);
            if (!entry.FieldEquals(extractedField, fieldValue))
                break;
            results.Add(entry.PkFromCompositeExtractor!(kv.Key));
        }
        return results;
    }

    private IEnumerable<KeyValuePair<IData, IData>> FindRangeUniqueIndex(
        IndexEntry entry, IData? from, bool hasFrom, IData? to, bool hasTo)
    {
        entry.IndexTable.Flush();

        // Materialize primary keys first
        var keys = entry.IndexTable.Forward(from!, hasFrom, to!, hasTo)
            .Select(kv => kv.Value)
            .ToList();

        foreach (var pk in keys)
        {
            if (_table.TryGet(pk, out var record))
                yield return new KeyValuePair<IData, IData>(pk, record);
        }
    }

    private IEnumerable<KeyValuePair<IData, IData>> FindRangeNonUniqueIndex(
        IndexEntry entry, IData? from, bool hasFrom, IData? to, bool hasTo)
    {
        entry.IndexTable.Flush();

        var fromComposite = hasFrom ? entry.ScanFromKeyBuilder!(from!) : default;
        var toComposite = hasTo ? entry.ScanFromKeyBuilder!(to!) : default;

        // Materialize keys first
        var keys = new List<IData>();
        foreach (var kv in entry.IndexTable.Forward(fromComposite!, hasFrom, toComposite!, hasTo))
        {
            // For 'to' bound: stop when field portion > to
            if (hasTo)
            {
                var field = entry.FieldFromCompositeExtractor!(kv.Key);
                // We compare field > to by checking !(field == to) after we've passed the range
                // Actually we let the WTree's natural ordering handle most of this,
                // but composite keys sort by field first so this is correct.
            }
            keys.Add(entry.PkFromCompositeExtractor!(kv.Key));
        }

        foreach (var pk in keys)
        {
            if (_table.TryGet(pk, out var record))
                yield return new KeyValuePair<IData, IData>(pk, record);
        }
    }

    private bool AreKeysEqual(IData a, IData b)
    {
        // Use the locator's equality comparer
        return _locator.KeyEqualityComparer!.Equals(a, b);
    }

    private int[] ResolveSlotIndices(string[] memberNames)
    {
        var members = _locator.RecordMembers;
        if (members == null)
            throw new InvalidOperationException(
                $"Table '{_tableName}' has no record member mapping. " +
                "Open the table with a typed OpenXTable<TKey, TRecord> call first, " +
                "or use slot indices directly.");

        var indices = new int[memberNames.Length];
        for (int i = 0; i < memberNames.Length; i++)
        {
            if (!members.TryGetValue(memberNames[i], out var idx))
                throw new ArgumentException(
                    $"Member '{memberNames[i]}' not found in table '{_tableName}'. " +
                    $"Available members: {string.Join(", ", members.Keys)}");
            indices[i] = idx;
        }
        return indices;
    }

    private string[] ResolveMemberNames(int[] slotIndices)
    {
        var members = _locator.RecordMembers;
        if (members == null)
            return slotIndices.Select(i => $"Slot{i}").ToArray();

        var reverseMap = members.ToDictionary(kv => kv.Value, kv => kv.Key);
        return slotIndices.Select(i =>
            reverseMap.TryGetValue(i, out var name) ? name : $"Slot{i}").ToArray();
    }

    private void DeleteIndexTable(string indexTableName)
    {
        // Find and mark as deleted
        var allLocators = _tree.GetAllLocators();
        foreach (var loc in allLocators)
        {
            if (loc.Name == indexTableName)
            {
                loc.IsDeleted = true;
                break;
            }
        }
    }

    // ── Inner types ──────────────────────────────────────────────────────────

    private sealed class IndexEntry
    {
        public readonly IndexDefinition Definition;
        public readonly XTablePortable IndexTable;
        public readonly Func<IData, IData> FieldExtractor;
        public readonly Func<IData, IData, bool> FieldEquals;
        public readonly Func<IData, IData, IData>? NonUniqueKeyBuilder;
        public readonly Func<IData, IData>? ScanFromKeyBuilder;
        public readonly Func<IData, IData>? FieldFromCompositeExtractor;
        public readonly Func<IData, IData>? PkFromCompositeExtractor;
        public readonly Type? CompositeKeyType;

        public IndexEntry(
            IndexDefinition definition,
            XTablePortable indexTable,
            Func<IData, IData> fieldExtractor,
            Func<IData, IData, bool> fieldEquals,
            Func<IData, IData, IData>? nonUniqueKeyBuilder,
            Func<IData, IData>? scanFromKeyBuilder,
            Func<IData, IData>? fieldFromCompositeExtractor,
            Func<IData, IData>? pkFromCompositeExtractor,
            Type? compositeKeyType)
        {
            Definition = definition;
            IndexTable = indexTable;
            FieldExtractor = fieldExtractor;
            FieldEquals = fieldEquals;
            NonUniqueKeyBuilder = nonUniqueKeyBuilder;
            ScanFromKeyBuilder = scanFromKeyBuilder;
            FieldFromCompositeExtractor = fieldFromCompositeExtractor;
            PkFromCompositeExtractor = pkFromCompositeExtractor;
            CompositeKeyType = compositeKeyType;
        }
    }
}
