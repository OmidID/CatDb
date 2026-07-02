// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;
using CatDb.Database.Querying;

namespace CatDb.Database.Indexing;

/// <summary>
/// Manages secondary indexes on a table at the IData level.
/// Accessible via <see cref="ITable.Indexes"/> on any table instance
/// (local, remote, typed, or portable).
/// </summary>
public interface ITableIndexManager
{
    /// <summary>
    /// Creates a secondary index on one or more record slots (fields).
    /// The slot indices correspond to Slot0, Slot1, ... in the record's Slots type,
    /// or can be resolved from property/member names via <see cref="IDescriptor.RecordMembers"/>.
    /// </summary>
    /// <param name="indexName">Unique name for this index.</param>
    /// <param name="slotIndices">The record slot indices to index (supports composite).</param>
    /// <param name="type">Whether the index enforces uniqueness.</param>
    /// <returns>A handle describing the created index.</returns>
    IndexDefinition CreateIndex(string indexName, int[] slotIndices, IndexType type);

    /// <summary>
    /// Creates a secondary index using record member (property) names.
    /// Names are resolved to slot indices via <see cref="IDescriptor.RecordMembers"/>.
    /// </summary>
    IndexDefinition CreateIndex(string indexName, string[] memberNames, IndexType type);

    /// <summary>Drops a named index and deletes its backing storage.</summary>
    void DropIndex(string indexName);

    /// <summary>Gets the definition of an existing index, or null if not found.</summary>
    IndexDefinition? GetIndex(string indexName);

    /// <summary>Lists all indexes on this table.</summary>
    IReadOnlyList<IndexDefinition> ListIndexes();

    /// <summary>Whether any indexes are currently registered.</summary>
    bool HasIndexes { get; }

    /// <summary>Rebuilds a specific index by scanning the entire main table.</summary>
    void RebuildIndex(string indexName);

    /// <summary>Rebuilds all indexes.</summary>
    void RebuildAllIndexes();

    // ── Search operations (IData level) ──────────────────────────────────────

    /// <summary>
    /// Finds all records whose indexed field(s) equal the given value.
    /// For single-slot indexes, <paramref name="fieldValue"/> is the slot value wrapped in Data&lt;T&gt;.
    /// For composite indexes, it is a Slots wrapping the indexed fields.
    /// </summary>
    IEnumerable<KeyValuePair<IData, IData>> FindByIndex(string indexName, IData fieldValue);

    /// <summary>Returns only the primary keys matching the indexed value.</summary>
    IEnumerable<IData> FindKeysByIndex(string indexName, IData fieldValue);

    /// <summary>
    /// Range search on an index, streamed in index order.
    /// <paramref name="backward"/> emits in descending field order (engine backward scan).
    /// Bounds honor inclusivity; results stream with bounded memory.
    /// </summary>
    IEnumerable<KeyValuePair<IData, IData>> FindByIndexRange(
        string indexName,
        IData? from, bool hasFrom, bool fromInclusive,
        IData? to, bool hasTo, bool toInclusive,
        bool backward);

    /// <summary>
    /// Streams records from a composite index restricted to a leading-field <b>prefix</b> value,
    /// ordered by the remaining indexed field(s). This is the engine plan for
    /// <c>WHERE a = v ORDER BY b</c> on a composite <c>(a, b)</c> index: a single ordered index
    /// range scan with no per-row residual or heap fetch beyond the matched rows.
    /// <paramref name="prefixFieldCount"/> is how many leading slots <paramref name="prefixValue"/>
    /// covers (currently 1). <paramref name="backward"/> reverses the trailing-field order.
    /// </summary>
    IEnumerable<KeyValuePair<IData, IData>> FindByIndexPrefix(
        string indexName, IData prefixValue, int prefixFieldCount, bool backward);

    /// <summary>Checks if a value exists in the named index.</summary>
    bool ExistsInIndex(string indexName, IData fieldValue);

    /// <summary>Counts entries matching the given value in the named index.</summary>
    long CountByIndex(string indexName, IData fieldValue);

    /// <summary>
    /// Executes a structured query <b>inside the engine</b>: resolves field predicates to index
    /// scans, intersects multiple indexes by primary key (AND), evaluates non-indexed predicates as
    /// a structured residual, and orders by the requested index/key fields. Streams matching
    /// <c>(primaryKey, record)</c> pairs honouring Skip/Take.
    /// </summary>
    IEnumerable<KeyValuePair<IData, IData>> ExecuteQuery(EngineQuery query);

    /// <summary>
    /// Counts a structured query's matching rows using the fastest available path — index-key-only
    /// counting when the plan allows (no per-row record fetch), falling back to full enumeration only
    /// when a residual predicate needs materialized rows. For a remote table this is a SINGLE round trip
    /// returning just the count — matched rows are never transferred over the wire.
    /// </summary>
    long Count(EngineQuery query);
}
