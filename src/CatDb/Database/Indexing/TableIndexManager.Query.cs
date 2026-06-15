// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections.Concurrent;
using CatDb.Data;
using CatDb.Database.Querying;

namespace CatDb.Database.Indexing;

/// <summary>
/// Engine-level query execution: <see cref="ExecuteQuery"/> compiles the structured
/// <see cref="EngineQuery"/> into a streaming physical plan (<see cref="QueryPlanner"/>) and runs it
/// against this manager via <see cref="IQueryEngineContext"/>. The manager is the storage-access
/// layer the plan operators call — index seeks, point fetches, table scans, field extraction.
/// </summary>
internal sealed partial class TableIndexManager : IQueryEngineContext
{
    private readonly ConcurrentDictionary<string, Func<IData, IData>> _extractorCache = new(StringComparer.Ordinal);

    /// <summary>Plan cache (SQL-Server style): one compiled, parameterized plan per query shape.</summary>
    private readonly ConcurrentDictionary<string, CompiledPlan> _planCache = new(StringComparer.Ordinal);

    public IEnumerable<KeyValuePair<IData, IData>> ExecuteQuery(EngineQuery query)
    {
        _table.Flush(); // indexes + main table must see committed writes before the plan streams
        var signature = QueryCompiler.Signature(query);
        var compiled = _planCache.GetOrAdd(signature, _ => QueryCompiler.Compile(query, this));
        var values = QueryCompiler.Collect(query);
        return compiled.Plan.Execute(new ParameterizedContext(this, values));
    }

    /// <summary>Counts a query's matching rows WITHOUT fetching records when the plan allows (e.g. an index
    /// seek/scan with no record-level residual). Returns null when the count needs materialized rows, so the
    /// caller falls back to enumeration. Avoids one main-table heap point-lookup per matching row — the
    /// difference between a cheap index-key count and O(matches) random reads on a large table.</summary>
    public long? TryCountFast(EngineQuery query)
    {
        _table.Flush();
        var signature = QueryCompiler.Signature(query);
        var compiled = _planCache.GetOrAdd(signature, _ => QueryCompiler.Compile(query, this));
        var values = QueryCompiler.Collect(query);
        return compiled.Plan.CountFast(new ParameterizedContext(this, values));
    }

    /// <summary>Returns the human-readable physical plan for a query (EXPLAIN).</summary>
    internal string ExplainQuery(EngineQuery query)
    {
        var signature = QueryCompiler.Signature(query);
        var compiled = _planCache.GetOrAdd(signature, _ => QueryCompiler.Compile(query, this));
        return compiled.Plan.Explain();
    }

    // ── IQueryEngineContext (storage access) ──────────────────────────────────

    string? IQueryEngineContext.ResolveIndex(string member)
        => ResolveSingleFieldIndex(member)?.Definition.Name;

    string? IQueryEngineContext.ResolveCoveringIndex(IReadOnlyList<string> members)
    {
        foreach (var entry in _indexes.Values)
        {
            var m = entry.Definition.MemberNames;
            if (m is null || m.Length != members.Count) continue;
            var match = true;
            for (var i = 0; i < m.Length; i++)
                if (!string.Equals(m[i], members[i], StringComparison.Ordinal)) { match = false; break; }
            if (match) return entry.Definition.Name;
        }
        return null;
    }

    // Value-independent (plan-cache friendly): unique index is maximally selective; others rank lower.
    long IQueryEngineContext.IndexSelectivity(string indexName)
        => _indexes.TryGetValue(indexName, out var e) && e.Definition.Type == IndexType.Unique ? 1 : 1000;

    // Literals are bound per-execution by ParameterizedContext; the bare manager never serves them.
    IData IQueryEngineContext.Parameter(int slot)
        => throw new InvalidOperationException("Query parameters are bound per execution.");

    IEnumerable<IData> IQueryEngineContext.SeekEqual(string indexName, IData value)
        => FindKeysByIndexCore(GetEntry(indexName), value);

    IEnumerable<IData> IQueryEngineContext.SeekRange(
        string indexName,
        IData? from, bool hasFrom, bool fromInclusive,
        IData? to, bool hasTo, bool toInclusive, bool backward)
    {
        var entry = GetEntry(indexName);
        return entry.Definition.Type == IndexType.Unique
            ? ScanUniqueRangeKeys(entry, from, hasFrom, fromInclusive, to, hasTo, toInclusive, backward)
            : ScanNonUniqueRangeKeys(entry, from, hasFrom, fromInclusive, to, hasTo, toInclusive, backward);
    }

    bool IQueryEngineContext.TryFetch(IData pk, out IData record) => _table.TryGet(pk, out record);

    IEnumerable<KeyValuePair<IData, IData>> IQueryEngineContext.ScanRows(
        IData? from, bool hasFrom, IData? to, bool hasTo, bool backward)
        => backward
            ? _table.Backward(to!, hasTo, from!, hasFrom)
            : _table.Forward(from!, hasFrom, to!, hasTo);

    Func<IData, IData> IQueryEngineContext.FieldExtractor(string member)
        => _extractorCache.GetOrAdd(member, m =>
        {
            var slot = ResolveSlotIndices([m])[0];
            return SlotAccessor.BuildExtractor(_locator.RecordType!, [slot]);
        });

    IComparer<IData> IQueryEngineContext.KeyComparer => _locator.KeyComparer!;
    IEqualityComparer<IData> IQueryEngineContext.KeyEqualityComparer => _locator.KeyEqualityComparer!;

    /// <summary>Finds a single-field index whose one member equals <paramref name="member"/>.</summary>
    private IndexEntry? ResolveSingleFieldIndex(string member)
    {
        foreach (var entry in _indexes.Values)
            if (entry.Definition.MemberNames is { Length: 1 } m &&
                string.Equals(m[0], member, StringComparison.Ordinal))
                return entry;
        return null;
    }

    // ── Field/key type resolvers for the remote protocol ──────────────────────

    /// <summary>CLR type of a record field by member name (for remote value decoding).</summary>
    internal Type GetMemberType(string member)
    {
        var slot = ResolveSlotIndices([member])[0];
        var dt = _locator.RecordDataType;
        return dt.IsPrimitive ? dt.PrimitiveType : dt[slot].PrimitiveType;
    }

    /// <summary>CLR type of the primary key (for remote key-range decoding).</summary>
    internal Type GetKeyType()
        => _locator.KeyType ?? _locator.KeyDataType.PrimitiveType;
}
