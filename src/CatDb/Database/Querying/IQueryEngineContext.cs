// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;

namespace CatDb.Database.Querying;

/// <summary>
/// Storage-access seam the planner and physical operators run against — the engine's equivalent of a
/// DBMS "access method" layer. Keeps the planner/executor independent of index/table internals so the
/// same plan code works for any backing store.
/// </summary>
internal interface IQueryEngineContext
{
    /// <summary>Name of a single-field index on <paramref name="member"/>, or null if none.</summary>
    string? ResolveIndex(string member);

    /// <summary>Name of an index whose member list equals <paramref name="members"/> in order (covers the
    /// sort/ORDER BY), or null. A single-member list matches a single-field index.</summary>
    string? ResolveCoveringIndex(IReadOnlyList<string> members);

    /// <summary>
    /// Cheap, value-independent selectivity rank for an index (smaller = more selective). Used to order
    /// an intersection's branches without touching data — so it is plan-cache friendly.
    /// </summary>
    long IndexSelectivity(string indexName);

    /// <summary>Bound query parameter value (literal) by slot — supplied per execution.</summary>
    IData Parameter(int slot);

    /// <summary>Primary keys whose indexed field equals <paramref name="value"/> (ordered by pk).</summary>
    IEnumerable<IData> SeekEqual(string indexName, IData value);

    /// <summary>Primary keys whose indexed field falls in the given range (index order).</summary>
    IEnumerable<IData> SeekRange(
        string indexName,
        IData? from, bool hasFrom, bool fromInclusive,
        IData? to, bool hasTo, bool toInclusive,
        bool backward);

    /// <summary>Primary keys from a COMPOSITE index whose leading <paramref name="prefixFieldCount"/> field(s)
    /// equal <paramref name="prefixValue"/>, streamed in the trailing field(s)' order (backward = descending).
    /// Serves <c>WHERE a=v ORDER BY b…</c> from an <c>(a,b…)</c> index WITHOUT fetching every match to sort —
    /// rows arrive already ordered, so only <c>Take</c> records are fetched.</summary>
    IEnumerable<IData> SeekPrefix(string indexName, IData prefixValue, int prefixFieldCount, bool backward);

    /// <summary>Point-fetch a record by primary key.</summary>
    bool TryFetch(IData pk, out IData record);

    /// <summary>Stream <c>(pk, record)</c> rows directly from the table, optionally key-bounded.</summary>
    IEnumerable<KeyValuePair<IData, IData>> ScanRows(
        IData? from, bool hasFrom, IData? to, bool hasTo, bool backward);

    /// <summary>Extractor pulling a record field (as <see cref="IData"/>) for residual/sort evaluation.</summary>
    Func<IData, IData> FieldExtractor(string member);

    IComparer<IData> KeyComparer { get; }
    IEqualityComparer<IData> KeyEqualityComparer { get; }
}
