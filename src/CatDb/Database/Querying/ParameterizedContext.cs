// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;

namespace CatDb.Database.Querying;

/// <summary>
/// Per-execution decorator that supplies the literal values for a <b>cached, parameterized</b> plan.
/// The plan (built once per query shape) references values by slot; this wraps the engine context for a
/// single run and resolves those slots — so the same compiled plan serves every value combination safely
/// across concurrent queries.
/// </summary>
internal sealed class ParameterizedContext(IQueryEngineContext inner, IData[] parameters) : IQueryEngineContext
{
    public IData Parameter(int slot) => parameters[slot];

    public string? ResolveIndex(string member) => inner.ResolveIndex(member);
    public string? ResolveCoveringIndex(IReadOnlyList<string> members) => inner.ResolveCoveringIndex(members);
    public long IndexSelectivity(string indexName) => inner.IndexSelectivity(indexName);
    public IEnumerable<IData> SeekEqual(string indexName, IData value) => inner.SeekEqual(indexName, value);

    public IEnumerable<IData> SeekRange(
        string indexName, IData? from, bool hasFrom, bool fromInclusive,
        IData? to, bool hasTo, bool toInclusive, bool backward)
        => inner.SeekRange(indexName, from, hasFrom, fromInclusive, to, hasTo, toInclusive, backward);

    public bool TryFetch(IData pk, out IData record) => inner.TryFetch(pk, out record);

    public IEnumerable<KeyValuePair<IData, IData>> ScanRows(
        IData? from, bool hasFrom, IData? to, bool hasTo, bool backward)
        => inner.ScanRows(from, hasFrom, to, hasTo, backward);

    public Func<IData, IData> FieldExtractor(string member) => inner.FieldExtractor(member);
    public IComparer<IData> KeyComparer => inner.KeyComparer;
    public IEqualityComparer<IData> KeyEqualityComparer => inner.KeyEqualityComparer;
}
