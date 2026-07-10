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
internal sealed partial class TableIndexManager : ITableIndexManager
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
        if (TryGetIdenticalOrThrow(indexName, slotIndices, type) is { } same)
            return same;

        var memberNames = ResolveMemberNames(slotIndices);
        var def = new IndexDefinition(indexName, slotIndices, memberNames, type);
        var entry = BuildEntry(def, out var preexisting);
        _indexes[indexName] = entry;
        _planCache.Clear(); // index set changed → cached plans may now be stale
        BackfillIfNeeded(entry, preexisting);
        return def;
    }

    public IndexDefinition CreateIndex(string indexName, string[] memberNames, IndexType type)
    {
        if (string.IsNullOrWhiteSpace(indexName))
            throw new ArgumentException("Index name cannot be empty.", nameof(indexName));
        if (memberNames == null || memberNames.Length == 0)
            throw new ArgumentException("Must specify at least one member name.", nameof(memberNames));

        var slotIndices = ResolveSlotIndices(memberNames);
        if (TryGetIdenticalOrThrow(indexName, slotIndices, type) is { } same)
            return same;

        var def = new IndexDefinition(indexName, slotIndices, memberNames, type);
        var entry = BuildEntry(def, out var preexisting);
        _indexes[indexName] = entry;
        _planCache.Clear();
        BackfillIfNeeded(entry, preexisting);
        return def;
    }

    /// <summary>
    /// Re-registering an index that already exists with the IDENTICAL definition is a no-op
    /// (SQL "IF NOT EXISTS" semantics) — the natural "ensure" pattern, and what a retried startup
    /// does. A DIFFERENT definition under the same name still throws.
    /// </summary>
    private IndexDefinition? TryGetIdenticalOrThrow(string indexName, int[] slotIndices, IndexType type)
    {
        if (!_indexes.TryGetValue(indexName, out var existing))
            return null;

        var def = existing.Definition;
        if (def.Type == type && def.SlotIndices.AsSpan().SequenceEqual(slotIndices))
            return def;

        throw new InvalidOperationException(
            $"Index '{indexName}' already exists on table '{_tableName}' with a different definition " +
            $"(existing: slots [{string.Join(",", def.SlotIndices)}] {def.Type}; " +
            $"requested: slots [{string.Join(",", slotIndices)}] {type}). Drop it first to redefine.");
    }

    /// <summary>
    /// A NEWLY created backing table for a main table that already holds rows must be built now —
    /// without this, creating an index on existing data (or after a schema migration dropped the
    /// stale index tables) left the index silently EMPTY: queries through it matched nothing.
    /// A REUSED (persisted) index table is verified instead of trusted: every index (unique or
    /// composite non-unique) holds exactly one entry per row, so an entry-count/row-count mismatch
    /// means the persisted index is not in step with the table (e.g. written by a version that did
    /// not flush index tables at commit) — rebuild it. Cost: one streaming count of each table at
    /// index registration; a persisted sync stamp is the follow-up optimization.
    /// </summary>
    private void BackfillIfNeeded(IndexEntry entry, bool preexistingIndexTable)
    {
        _table.Flush();

        if (preexistingIndexTable)
        {
            entry.IndexTable.Flush();
            if (entry.IndexTable.Count() == _table.Count())
                return; // in step — reuse as-is
        }

        if (_table.Forward().Any())
            RebuildEntry(entry);
        else
        {
            // Empty main table: make sure a stale persisted index doesn't resurrect ghost rows.
            entry.IndexTable.Clear();
            entry.IndexTable.Flush();
        }
    }

    public void DropIndex(string indexName)
    {
        if (!_indexes.TryGetValue(indexName, out var entry))
            throw new KeyNotFoundException($"Index '{indexName}' not found on table '{_tableName}'.");

        entry.IndexTable.Clear();
        entry.IndexTable.Flush();

        // Mark the index table's locator as deleted
        var indexTableName = entry.Definition.GetTableName(_tableName);
        if (_tree is StorageEngine engine)
            engine.RemoveInternalTable(indexTableName); // clears + soft-deletes + unmaps
        else
            DeleteIndexTable(indexTableName);

        _indexes.Remove(indexName);
        _planCache.Clear();
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
    //
    // All index reads STREAM. They never hold the index-table scan open while doing a
    // main-table point lookup: a writer locks main→index, so a reader holding the index
    // scan open and then reading main would invert the order and deadlock. BatchedScan
    // re-seeks page-by-page (each page fully closes its enumerator before any record
    // fetch), so only one table lock is ever held at a time and memory is bounded to one
    // page — not the whole result set.

    /// <summary>Page size for streaming index scans (rows of index keys buffered per hop).</summary>
    private const int ScanBatchSize = 4096;

    private IndexEntry GetEntry(string indexName) =>
        _indexes.TryGetValue(indexName, out var entry)
            ? entry
            : throw new KeyNotFoundException($"Index '{indexName}' not found.");

    public IEnumerable<KeyValuePair<IData, IData>> FindByIndex(string indexName, IData fieldValue)
    {
        var entry = GetEntry(indexName);
        _table.Flush(); // ensure index sees committed writes
        return StreamRecords(FindKeysByIndexCore(entry, fieldValue));
    }

    public IEnumerable<IData> FindKeysByIndex(string indexName, IData fieldValue)
    {
        var entry = GetEntry(indexName);
        _table.Flush();
        return FindKeysByIndexCore(entry, fieldValue);
    }

    private IEnumerable<IData> FindKeysByIndexCore(IndexEntry entry, IData fieldValue)
    {
        fieldValue = entry.NormalizeFieldValue(fieldValue); // enum / Nullable<T> → storage form
        return entry.Definition.Type == IndexType.Unique
            ? FindKeysByUniqueIndex(entry, fieldValue)
            : FindKeysByNonUniqueIndex(entry, fieldValue);
    }

    public IEnumerable<KeyValuePair<IData, IData>> FindByIndexRange(
        string indexName,
        IData? from, bool hasFrom, bool fromInclusive,
        IData? to, bool hasTo, bool toInclusive,
        bool backward)
    {
        var entry = GetEntry(indexName);
        _table.Flush();
        var keys = entry.Definition.Type == IndexType.Unique
            ? ScanUniqueRangeKeys(entry, from, hasFrom, fromInclusive, to, hasTo, toInclusive, backward)
            : ScanNonUniqueRangeKeys(entry, from, hasFrom, fromInclusive, to, hasTo, toInclusive, backward);
        return StreamRecords(keys);
    }

    public IEnumerable<KeyValuePair<IData, IData>> FindByIndexPrefix(
        string indexName, IData prefixValue, int prefixFieldCount, bool backward)
    {
        var entry = GetEntry(indexName);
        if (entry.CompositeKeyType is null)
            throw new InvalidOperationException(
                $"Index '{indexName}' is not a composite index; prefix scan is not applicable.");
        if (prefixFieldCount < 1 || prefixFieldCount >= entry.Definition.SlotIndices.Length)
            throw new ArgumentOutOfRangeException(nameof(prefixFieldCount),
                "Prefix must cover at least one and fewer than all indexed fields.");
        _table.Flush();
        return StreamRecords(ScanPrefixKeys(entry, prefixValue, prefixFieldCount, backward));
    }

    /// <summary>
    /// Streams primary keys whose composite index entry's leading <paramref name="prefixLen"/>
    /// field(s) equal <paramref name="prefixValue"/>, ordered by the trailing field(s). The prefix
    /// block is bracketed by exact sentinel bounds [(prefix, MIN…), (prefix, MAX…)] so BOTH ends are
    /// WTree seeks (forward and backward), not tail scans. Falls back to a filtered length-1 scan
    /// only when a trailing slot type lacks a max sentinel (e.g. string).
    /// </summary>
    private static IEnumerable<IData> ScanPrefixKeys(IndexEntry entry, IData prefixValue, int prefixLen, bool backward)
    {
        entry.IndexTable.Flush();
        if (prefixLen == 1) prefixValue = entry.NormalizeLeadValue(prefixValue); // enum / Nullable<T> → storage form
        var getPk = entry.PkFromCompositeExtractor!;

        var lo = BuildCompositeBound(entry, prefixValue, prefixLen, fillMax: false);
        var hi = BuildCompositeBound(entry, prefixValue, prefixLen, fillMax: true);
        if (lo is not null && hi is not null)
        {
            foreach (var kv in BatchedScan(entry.IndexTable, lo, true, hi, true, backward))
                yield return getPk(kv.Key);
            yield break;
        }

        // Fallback: trailing type has no max sentinel (string). Only length-1 prefixes have the
        // compiled extractor needed for the filtered path.
        if (prefixLen != 1 || entry.PrefixExtractor is null)
            throw new NotSupportedException(
                "Prefix scan requires sentinel-seekable trailing types for multi-field prefixes.");

        var getLead = entry.PrefixExtractor;
        var leadCmp = entry.PrefixComparer!;
        if (!backward)
        {
            var seek = entry.PrefixSeekBuilder!(prefixValue);
            foreach (var kv in BatchedScan(entry.IndexTable, seek, true, null, false, backward: false))
            {
                var c = leadCmp.Compare(getLead(kv.Key), prefixValue);
                if (c < 0) continue;
                if (c > 0) yield break;
                yield return getPk(kv.Key);
            }
        }
        else
        {
            foreach (var kv in BatchedScan(entry.IndexTable, null, false, null, false, backward: true))
            {
                var c = leadCmp.Compare(getLead(kv.Key), prefixValue);
                if (c > 0) continue;
                if (c < 0) yield break;
                yield return getPk(kv.Key);
            }
        }
    }

    public bool ExistsInIndex(string indexName, IData fieldValue)
    {
        var entry = GetEntry(indexName);
        _table.Flush();
        return entry.Definition.Type == IndexType.Unique
            ? entry.IndexTable.Exists(fieldValue)
            : FindKeysByNonUniqueIndex(entry, fieldValue).Any();
    }

    public long CountByIndex(string indexName, IData fieldValue)
    {
        var entry = GetEntry(indexName);
        _table.Flush();
        return entry.Definition.Type == IndexType.Unique
            ? entry.IndexTable.Exists(fieldValue) ? 1 : 0
            : FindKeysByNonUniqueIndex(entry, fieldValue).LongCount();
    }

    /// <summary>
    /// Streams (primaryKey → record) by point-looking-up each primary key. The
    /// <paramref name="primaryKeys"/> source must itself be lock-release-between-yields
    /// (i.e. BatchedScan-backed), so no index lock is held while we read the main table.
    /// </summary>
    private IEnumerable<KeyValuePair<IData, IData>> StreamRecords(IEnumerable<IData> primaryKeys)
    {
        foreach (var pk in primaryKeys)
            if (_table.TryGet(pk, out var record))
                yield return new KeyValuePair<IData, IData>(pk, record);
    }

    /// <summary>
    /// Forward/backward streaming scan over an index table that NEVER holds the scan open
    /// across yields. Each page is materialized then its enumerator disposed (releasing the
    /// table lock); the next page re-seeks from the last key (inclusive) and drops the one
    /// duplicate boundary entry. Bounds are inclusive in index-key space.
    /// </summary>
    private static IEnumerable<KeyValuePair<IData, IData>> BatchedScan(
        XTablePortable table, IData? lo, bool hasLo, IData? hi, bool hasHi, bool backward)
    {
        var cmp = table.Locator.KeyComparer!;
        var cursor = backward ? hi : lo;
        var hasCursor = backward ? hasHi : hasLo;
        var first = true;

        while (true)
        {
            // .Take().ToList() fully drains+disposes the Forward/Backward iterator,
            // so the index-table lock is released before we return any element.
            var page = backward
                ? table.Backward(cursor!, hasCursor, lo!, hasLo).Take(ScanBatchSize).ToList()
                : table.Forward(cursor!, hasCursor, hi!, hasHi).Take(ScanBatchSize).ToList();

            if (page.Count == 0)
                yield break;

            var start = 0;
            if (!first)
                while (start < page.Count && cmp.Compare(page[start].Key, cursor!) == 0)
                    start++; // skip the re-seek boundary duplicate (index keys are unique)

            for (var i = start; i < page.Count; i++)
                yield return page[i];

            if (page.Count < ScanBatchSize)
                yield break; // last (partial) page

            cursor = page[^1].Key;
            hasCursor = true;
            first = false;
        }
    }

    private IEnumerable<IData> ScanUniqueRangeKeys(
        IndexEntry entry,
        IData? from, bool hasFrom, bool fromInclusive,
        IData? to, bool hasTo, bool toInclusive,
        bool backward)
    {
        entry.IndexTable.Flush();
        var eq = entry.FieldEquals;

        if (hasFrom) from = entry.NormalizeFieldValue(from!); // enum / Nullable<T> → storage form
        if (hasTo) to = entry.NormalizeFieldValue(to!);

        // Unique index key IS the field value, so range bounds map directly.
        foreach (var kv in BatchedScan(entry.IndexTable, from, hasFrom, to, hasTo, backward))
        {
            var field = kv.Key;
            if (hasFrom && !fromInclusive && eq(field, from!)) continue;
            if (hasTo && !toInclusive && eq(field, to!)) continue;
            yield return kv.Value; // stored primary key
        }
    }

    private IEnumerable<IData> ScanNonUniqueRangeKeys(
        IndexEntry entry,
        IData? from, bool hasFrom, bool fromInclusive,
        IData? to, bool hasTo, bool toInclusive,
        bool backward)
    {
        entry.IndexTable.Flush();
        var fieldCount = entry.Definition.SlotIndices.Length;

        if (hasFrom) from = entry.NormalizeFieldValue(from!); // enum / Nullable<T> → storage form
        if (hasTo) to = entry.NormalizeFieldValue(to!);

        // Encode field-level inclusivity into the trailing primary-key sentinel so the WHOLE bound
        // is a single composite key the WTree can seek to:
        //   field >= from → (from, MIN_pk) ;  field >  from → (from, MAX_pk)
        //   field <= to   → (to,   MAX_pk) ;  field <  to   → (to,   MIN_pk)
        var lo = hasFrom ? BuildCompositeBound(entry, from!, fieldCount, fillMax: !fromInclusive) : null;
        var hi = hasTo ? BuildCompositeBound(entry, to!, fieldCount, fillMax: toInclusive) : null;

        var getPk = entry.PkFromCompositeExtractor!;
        if ((!hasFrom || lo is not null) && (!hasTo || hi is not null))
        {
            // Pure WTree-seek range scan: Forward/Backward stop at the bounds natively — no skipping.
            foreach (var kv in BatchedScan(entry.IndexTable, lo, hasFrom, hi, hasTo, backward))
                yield return getPk(kv.Key);
            yield break;
        }

        // Fallback (a trailing type has no sentinel, e.g. string primary key): seek the available end
        // and bracket the other by field comparison.
        foreach (var pk in ScanNonUniqueRangeKeysFiltered(entry, from, hasFrom, fromInclusive, to, hasTo, toInclusive, backward))
            yield return pk;
    }

    private IEnumerable<IData> ScanNonUniqueRangeKeysFiltered(
        IndexEntry entry,
        IData? from, bool hasFrom, bool fromInclusive,
        IData? to, bool hasTo, bool toInclusive,
        bool backward)
    {
        var fcmp = entry.FieldComparer;
        var getField = entry.FieldFromCompositeExtractor!;
        var getPk = entry.PkFromCompositeExtractor!;

        if (!backward)
        {
            var lo = hasFrom ? entry.ScanFromKeyBuilder!(from!) : null;
            foreach (var kv in BatchedScan(entry.IndexTable, lo, hasFrom, null, false, backward: false))
            {
                var field = getField(kv.Key);
                if (hasFrom)
                {
                    var c = fcmp.Compare(field, from!);
                    if (c < 0) continue;
                    if (c == 0 && !fromInclusive) continue;
                }
                if (hasTo)
                {
                    var c = fcmp.Compare(field, to!);
                    if (c > 0) yield break;
                    if (c == 0 && !toInclusive) continue;
                }
                yield return getPk(kv.Key);
            }
        }
        else
        {
            foreach (var kv in BatchedScan(entry.IndexTable, null, false, null, false, backward: true))
            {
                var field = getField(kv.Key);
                if (hasTo)
                {
                    var c = fcmp.Compare(field, to!);
                    if (c > 0) continue;
                    if (c == 0 && !toInclusive) continue;
                }
                if (hasFrom)
                {
                    var c = fcmp.Compare(field, from!);
                    if (c < 0) yield break;
                    if (c == 0 && !fromInclusive) continue;
                }
                yield return getPk(kv.Key);
            }
        }
    }

    /// <summary>
    /// Builds a composite index key whose leading <paramref name="leadingCount"/> slots come from
    /// <paramref name="leading"/> (a Data&lt;field&gt; / Data&lt;Slots&lt;…&gt;&gt;) and whose
    /// trailing slots are filled with the min (or max) sentinel — the exact lower/upper bound for a
    /// WTree seek. Returns null if any trailing slot type has no sentinel.
    /// </summary>
    private static IData? BuildCompositeBound(IndexEntry entry, IData leading, int leadingCount, bool fillMax)
    {
        var compositeType = entry.CompositeKeyType!;
        var ctor = compositeType.GetConstructors()
            .OrderByDescending(c => c.GetParameters().Length).First();
        var ps = ctor.GetParameters();
        var args = new object?[ps.Length];

        // IData = object: leading IS the field value (no Data<T> wrapper to unwrap)
        if (leadingCount == 1)
        {
            args[0] = leading;
        }
        else
        {
            // leading is Slots<T0,T1,...> — extract each slot via reflection
            for (var i = 0; i < leadingCount; i++)
                args[i] = leading.GetType().GetField($"Slot{i}")!.GetValue(leading);
        }

        for (var i = leadingCount; i < ps.Length; i++)
        {
            var slotType = SlotAccessor.GetSlotType(compositeType, i);
            if (!Sentinels.TryGet(slotType, fillMax, out var sentinel))
                return null;
            args[i] = sentinel;
        }

        var composite = ctor.Invoke(args);
        return composite;
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

    private static readonly IData _dummyValue = (object)(byte)0;

    /// <summary>
    /// Opens the backing index table. Through a <see cref="StorageEngine"/> this REUSES the
    /// persisted locator when its schema still matches (index data survives restarts, and the
    /// table is enrolled in the engine map so commits flush it); <paramref name="preexisting"/>
    /// then tells <see cref="CreateIndex(string,int[],IndexType)"/> whether a backfill is needed.
    /// </summary>
    private XTablePortable ObtainIndexTable(
        string indexTableName, DataType keyDataType, DataType recordDataType, Type keyType, Type recordType,
        out bool preexisting)
    {
        if (_tree is StorageEngine engine)
            return engine.ObtainInternalTable(indexTableName, keyDataType, recordDataType, keyType, recordType, out preexisting);

        // Raw WTree (no engine surface): legacy path — always a fresh locator.
        preexisting = false;
        var idxLocator = _tree.CreateLocator(indexTableName, StructureType.XTABLE,
            keyDataType, recordDataType, keyType, recordType);
        if (!idxLocator.IsReady) idxLocator.Prepare();
        return new XTablePortable(_tree, idxLocator);
    }

    private IndexEntry BuildEntry(IndexDefinition def) => BuildEntry(def, out _);

    private IndexEntry BuildEntry(IndexDefinition def, out bool preexistingIndexTable)
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

        // Index field types are driven by the record members' NORMALIZED storage types
        // (enum → underlying, Nullable<T> → T) so the extracted/queried values match the index
        // table's regenerated-on-reopen CLR key type. See SlotAccessor.NormalizeStorageType.
        Type[] rawTypes;
        Type[] storageTypes;
        if (recordDataType.IsPrimitive && def.SlotIndices.Length == 1 && def.SlotIndices[0] == 0)
        {
            // Primitive record — identity extraction
            fieldExtractor = r => r;
            rawTypes = [recordType];
            storageTypes = [SlotAccessor.NormalizeStorageType(recordType)];
            fieldType = storageTypes[0];
            fieldDataType = DataTypeUtils.BuildDataType(fieldType);
        }
        else
        {
            fieldExtractor = SlotAccessor.BuildExtractor(recordType, def.SlotIndices);
            rawTypes = def.SlotIndices.Select(i => SlotAccessor.GetRecordMemberRawType(recordType, i)).ToArray();
            storageTypes = def.SlotIndices.Select(i => SlotAccessor.GetRecordMemberStorageType(recordType, i)).ToArray();
            fieldType = storageTypes.Length == 1 ? storageTypes[0] : SlotsBuilder.BuildType(storageTypes);
            fieldDataType = DataTypeUtils.BuildDataType(fieldType);
        }

        // Query-value normalizers: convert user-supplied enum / Nullable<T> field values to the
        // index's normalized storage form. Identity (no-op) for plain primitive/string fields.
        var normalizeFieldValue = def.SlotIndices.Length == 1
            ? SlotAccessor.BuildValueNormalizer(rawTypes[0])
            : (Func<IData, IData>)(v => v);
        var normalizeLeadValue = SlotAccessor.BuildValueNormalizer(rawTypes[0]);

        // Build field equality + ordering comparers (ordering drives range stop conditions).
        var fieldEquals = SlotAccessor.BuildFieldEqualityComparer(fieldType);
        var fieldComparer = new DataComparer(fieldType);

        XTablePortable indexTable;
        Func<IData, IData, IData>? nonUniqueKeyBuilder = null;
        Func<IData, IData>? scanFromKeyBuilder = null;
        Func<IData, IData>? fieldFromCompositeExtractor = null;
        Func<IData, IData>? pkFromCompositeExtractor = null;
        Type? compositeKeyType = null;
        // Prefix (leading single field) tooling — only built for composite indexes (≥2 fields).
        Func<IData, IData>? prefixExtractor = null;
        Func<IData, IData>? prefixSeekBuilder = null;
        IComparer<IData>? prefixComparer = null;

        var preexisting = false;
        if (def.Type == IndexType.Unique)
        {
            // Unique: index table key = field value, value = primary key
            indexTable = ObtainIndexTable(indexTableName, fieldDataType, keyDataType, fieldType, keyType, out preexisting);
        }
        else
        {
            // Non-unique: index table key = Slots(field..., primaryKey), value = byte.
            // Built from normalized storage types so it matches the reopen-regenerated CLR key type.
            var compositeStorageTypes = storageTypes
                .Append(SlotAccessor.NormalizeStorageType(keyType))
                .ToArray();
            compositeKeyType = SlotsBuilder.BuildType(compositeStorageTypes);
            var compositeKeyDataType = DataTypeUtils.BuildDataType(compositeKeyType);

            indexTable = ObtainIndexTable(indexTableName, compositeKeyDataType, DataType.Byte, compositeKeyType, typeof(byte), out preexisting);

            nonUniqueKeyBuilder = SlotAccessor.BuildNonUniqueKeyBuilder(
                recordType, def.SlotIndices, keyType);
            scanFromKeyBuilder = SlotAccessor.BuildScanFromKeyBuilder(
                compositeKeyType, fieldType, keyType, def.SlotIndices.Length);
            fieldFromCompositeExtractor = SlotAccessor.BuildFieldExtractorFromCompositeKey(
                compositeKeyType, def.SlotIndices.Length);
            pkFromCompositeExtractor = SlotAccessor.BuildPrimaryKeyExtractorFromCompositeKey(
                compositeKeyType, def.SlotIndices.Length + 1);

            if (def.SlotIndices.Length >= 2)
            {
                // Leading-field (prefix length 1) tooling for composite prefix scans.
                var leadingType = SlotAccessor.GetSlotType(compositeKeyType, 0);
                prefixExtractor = SlotAccessor.BuildFieldExtractorFromCompositeKey(compositeKeyType, 1);
                prefixSeekBuilder = SlotAccessor.BuildPrefixSeekKeyBuilder(compositeKeyType, leadingType, 1);
                prefixComparer = new DataComparer(leadingType);
            }
        }

        preexistingIndexTable = preexisting;
        return new IndexEntry(
            def, indexTable, fieldExtractor, fieldEquals, fieldComparer,
            nonUniqueKeyBuilder, scanFromKeyBuilder,
            fieldFromCompositeExtractor, pkFromCompositeExtractor,
            compositeKeyType, prefixExtractor, prefixSeekBuilder, prefixComparer, fieldType,
            normalizeFieldValue, normalizeLeadValue);
    }

    /// <summary>The .NET type of an index's field value (single-slot scalar or composite Slots).</summary>
    internal Type GetFieldType(string indexName) => GetEntry(indexName).FieldType;

    /// <summary>The .NET type of the leading <paramref name="prefixLen"/> slots of a composite index.</summary>
    internal Type GetPrefixType(string indexName, int prefixLen)
    {
        var entry = GetEntry(indexName);
        if (entry.CompositeKeyType is null)
            throw new InvalidOperationException($"Index '{indexName}' is not composite.");
        if (prefixLen == 1)
            return SlotAccessor.GetSlotType(entry.CompositeKeyType, 0);
        var types = Enumerable.Range(0, prefixLen)
            .Select(i => SlotAccessor.GetSlotType(entry.CompositeKeyType, i)).ToArray();
        return SlotsBuilder.BuildType(types);
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

    private static IEnumerable<IData> FindKeysByUniqueIndex(IndexEntry entry, IData fieldValue)
    {
        entry.IndexTable.Flush();
        // O(log N) point lookup — releases the lock before yielding, so StreamRecords is safe.
        if (entry.IndexTable.TryGet(fieldValue, out var primaryKey))
            yield return primaryKey;
    }

    private static IEnumerable<IData> FindKeysByNonUniqueIndex(IndexEntry entry, IData fieldValue)
    {
        entry.IndexTable.Flush();
        var fieldCount = entry.Definition.SlotIndices.Length;
        var getPk = entry.PkFromCompositeExtractor!;

        // Equal-field block = [(field, MIN_pk), (field, MAX_pk)] — a single WTree-seekable range
        // that includes negative/min primary keys (the old (field, defaultPk) lower bound skipped
        // any pk < default).
        var lo = BuildCompositeBound(entry, fieldValue, fieldCount, fillMax: false);
        var hi = BuildCompositeBound(entry, fieldValue, fieldCount, fillMax: true);
        if (lo is not null && hi is not null)
        {
            foreach (var kv in BatchedScan(entry.IndexTable, lo, true, hi, true, backward: false))
                yield return getPk(kv.Key);
            yield break;
        }

        // Fallback (string primary key — no max sentinel): seek the field block and stop on change.
        var fromKey = entry.ScanFromKeyBuilder!(fieldValue);
        foreach (var kv in BatchedScan(entry.IndexTable, fromKey, true, null, false, backward: false))
        {
            if (!entry.FieldEquals(entry.FieldFromCompositeExtractor!(kv.Key), fieldValue))
                yield break;
            yield return getPk(kv.Key);
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
        public readonly IComparer<IData> FieldComparer;
        public readonly Func<IData, IData, IData>? NonUniqueKeyBuilder;
        public readonly Func<IData, IData>? ScanFromKeyBuilder;
        public readonly Func<IData, IData>? FieldFromCompositeExtractor;
        public readonly Func<IData, IData>? PkFromCompositeExtractor;
        public readonly Type? CompositeKeyType;
        // Prefix-length-1 tooling for composite prefix scans (null for unique / single-field).
        public readonly Func<IData, IData>? PrefixExtractor;
        public readonly Func<IData, IData>? PrefixSeekBuilder;
        public readonly IComparer<IData>? PrefixComparer;
        public readonly Type FieldType;
        // Normalize user-supplied query values (enum / Nullable<T>) to the index storage form.
        // NormalizeFieldValue: whole single-field value (identity for composite / plain fields).
        // NormalizeLeadValue: the leading field value (used by prefix / composite-lead seeks).
        public readonly Func<IData, IData> NormalizeFieldValue;
        public readonly Func<IData, IData> NormalizeLeadValue;

        public IndexEntry(
            IndexDefinition definition,
            XTablePortable indexTable,
            Func<IData, IData> fieldExtractor,
            Func<IData, IData, bool> fieldEquals,
            IComparer<IData> fieldComparer,
            Func<IData, IData, IData>? nonUniqueKeyBuilder,
            Func<IData, IData>? scanFromKeyBuilder,
            Func<IData, IData>? fieldFromCompositeExtractor,
            Func<IData, IData>? pkFromCompositeExtractor,
            Type? compositeKeyType,
            Func<IData, IData>? prefixExtractor,
            Func<IData, IData>? prefixSeekBuilder,
            IComparer<IData>? prefixComparer,
            Type fieldType,
            Func<IData, IData> normalizeFieldValue,
            Func<IData, IData> normalizeLeadValue)
        {
            Definition = definition;
            IndexTable = indexTable;
            FieldExtractor = fieldExtractor;
            FieldEquals = fieldEquals;
            FieldComparer = fieldComparer;
            NonUniqueKeyBuilder = nonUniqueKeyBuilder;
            ScanFromKeyBuilder = scanFromKeyBuilder;
            FieldFromCompositeExtractor = fieldFromCompositeExtractor;
            PkFromCompositeExtractor = pkFromCompositeExtractor;
            CompositeKeyType = compositeKeyType;
            PrefixExtractor = prefixExtractor;
            PrefixSeekBuilder = prefixSeekBuilder;
            PrefixComparer = prefixComparer;
            FieldType = fieldType;
            NormalizeFieldValue = normalizeFieldValue;
            NormalizeLeadValue = normalizeLeadValue;
        }
    }
}
