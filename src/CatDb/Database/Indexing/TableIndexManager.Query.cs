// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;
using CatDb.Database.Querying;

namespace CatDb.Database.Indexing;

/// <summary>
/// Engine-level structured query execution: the engine itself resolves field predicates to index
/// scans, <b>intersects multiple indexes by primary key</b> (AND), evaluates non-indexed predicates
/// as a structured residual (field extracted from the record and compared by the engine's
/// <see cref="DataComparer"/> — not a caller delegate), and sorts the result by index/key fields.
/// This is the real query path; the fluent builder is only a thin spec producer over it.
/// </summary>
internal sealed partial class TableIndexManager
{
    /// <summary>
    /// Executes a structured query and streams matching <c>(primaryKey, record)</c> pairs in the
    /// requested order, honouring Skip/Take.
    /// </summary>
    public IEnumerable<KeyValuePair<IData, IData>> ExecuteQuery(EngineQuery query)
    {
        _table.Flush(); // indexes + main table must see committed writes

        // Streaming fast path: no predicates and ordering by primary key (or none) → iterate the
        // WTree directly forward/backward, no buffering. Keeps full-table key scans O(1) memory.
        if (query.Filters.Count == 0 && !query.HasKeyRange &&
            (query.Sorts.Count == 0 || (query.Sorts.Count == 1 && query.Sorts[0].Member is null)))
        {
            var descending = query.Sorts.Count == 1 && query.Sorts[0].Descending;
            var stream = descending ? _table.Backward() : _table.Forward();
            if (query.Skip > 0) stream = stream.Skip(query.Skip);
            if (query.Take.HasValue) stream = stream.Take(query.Take.Value);
            return stream;
        }

        // 1) Split predicates: those an index can drive vs. those evaluated as a residual.
        var indexed = new List<(FieldFilter Filter, IndexEntry Entry)>();
        var residual = new List<FieldFilter>();
        foreach (var f in query.Filters)
        {
            var entry = ResolveSingleFieldIndex(f.Member);
            if (entry is not null && f.Op != FilterOp.Prefix)
                indexed.Add((f, entry));
            else
                residual.Add(f);
        }

        var residualEvaluators = residual.Select(BuildResidualEvaluator).ToArray();
        var keyInRange = BuildKeyRangePredicate(query);
        var rows = new List<KeyValuePair<IData, IData>>();

        if (indexed.Count == 0)
        {
            // No index-resolvable predicate → scan the table directly (records in hand, no TryGet
            // while the scan holds its read lock). A key range narrows the scan at the engine.
            var scan = query.HasKeyRange
                ? _table.Forward(query.KeyFrom!, query.HasKeyFrom, query.KeyTo!, query.HasKeyTo)
                : _table.Forward();
            foreach (var kv in scan)
                if ((keyInRange is null || keyInRange(kv.Key)) && PassesResidual(kv.Value, residualEvaluators))
                    rows.Add(kv);
        }
        else
        {
            // 2) Candidate primary keys: intersect every indexed predicate's pk set (AND).
            //    Materialize first, then point-fetch records (separate table locks, no re-entrancy).
            //    A key range becomes a residual on the primary key.
            var candidates = IntersectIndexed(indexed);
            foreach (var pk in candidates)
            {
                if (keyInRange is not null && !keyInRange(pk)) continue;
                if (!_table.TryGet(pk, out var record)) continue;
                if (PassesResidual(record, residualEvaluators))
                    rows.Add(new KeyValuePair<IData, IData>(pk, record));
            }
        }

        // 4) Order (engine-level): by the requested sort keys, else by primary key for determinism.
        rows.Sort(BuildRowComparer(query.Sorts));

        // 5) Skip / Take.
        IEnumerable<KeyValuePair<IData, IData>> result = rows;
        if (query.Skip > 0) result = result.Skip(query.Skip);
        if (query.Take.HasValue) result = result.Take(query.Take.Value);
        return result;
    }

    // ── Predicate → index resolution ──────────────────────────────────────────

    /// <summary>Finds a single-field index whose one member equals <paramref name="member"/>.</summary>
    private IndexEntry? ResolveSingleFieldIndex(string member)
    {
        foreach (var entry in _indexes.Values)
            if (entry.Definition.MemberNames is { Length: 1 } m &&
                string.Equals(m[0], member, StringComparison.Ordinal))
                return entry;
        return null;
    }

    /// <summary>Primary-key stream for one indexed predicate (equality or range).</summary>
    private IEnumerable<IData> KeysForFilter(FieldFilter f, IndexEntry entry)
    {
        switch (f.Op)
        {
            case FilterOp.Equal:
                return FindKeysByIndexCore(entry, f.Value!);
            case FilterOp.AtLeast:
            case FilterOp.GreaterThan:
                return RangeKeys(entry, f.Value, true, f.Op == FilterOp.AtLeast, null, false, true);
            case FilterOp.AtMost:
            case FilterOp.LessThan:
                return RangeKeys(entry, null, false, true, f.Value, true, f.Op == FilterOp.AtMost);
            case FilterOp.Between:
                return RangeKeys(entry, f.Value, true, f.FromInclusive, f.Value2, true, f.ToInclusive);
            default:
                throw new NotSupportedException($"Operator {f.Op} is not index-resolvable.");
        }
    }

    private IEnumerable<IData> RangeKeys(
        IndexEntry entry,
        IData? from, bool hasFrom, bool fromInclusive,
        IData? to, bool hasTo, bool toInclusive)
        => entry.Definition.Type == IndexType.Unique
            ? ScanUniqueRangeKeys(entry, from, hasFrom, fromInclusive, to, hasTo, toInclusive, backward: false)
            : ScanNonUniqueRangeKeys(entry, from, hasFrom, fromInclusive, to, hasTo, toInclusive, backward: false);

    // ── Multi-index AND intersection (by primary key) ──────────────────────────

    private IEnumerable<IData> IntersectIndexed(List<(FieldFilter Filter, IndexEntry Entry)> indexed)
    {
        if (indexed.Count == 1)
            return KeysForFilter(indexed[0].Filter, indexed[0].Entry);

        // Materialize each predicate's pk set, then intersect smallest-first (hash intersection).
        var eq = _locator.KeyEqualityComparer!;
        var sets = new List<HashSet<IData>>(indexed.Count);
        foreach (var (filter, entry) in indexed)
            sets.Add(new HashSet<IData>(KeysForFilter(filter, entry), eq));

        sets.Sort((a, b) => a.Count.CompareTo(b.Count));
        var acc = sets[0];
        for (var i = 1; i < sets.Count && acc.Count > 0; i++)
            acc.IntersectWith(sets[i]);
        return acc;
    }

    /// <summary>Predicate enforcing the primary-key range (incl. exclusivity), or null if none.</summary>
    private Func<IData, bool>? BuildKeyRangePredicate(EngineQuery q)
    {
        if (!q.HasKeyRange) return null;
        var cmp = _locator.KeyComparer!;
        return pk =>
        {
            if (q.HasKeyFrom)
            {
                var c = cmp.Compare(pk, q.KeyFrom!);
                if (c < 0 || (c == 0 && !q.KeyFromInclusive)) return false;
            }
            if (q.HasKeyTo)
            {
                var c = cmp.Compare(pk, q.KeyTo!);
                if (c > 0 || (c == 0 && !q.KeyToInclusive)) return false;
            }
            return true;
        };
    }

    private static bool PassesResidual(IData record, Func<IData, bool>[] evaluators)
    {
        foreach (var ev in evaluators)
            if (!ev(record)) return false;
        return true;
    }

    // ── Structured residual evaluation (engine-side, no caller delegate) ────────

    private Func<IData, bool> BuildResidualEvaluator(FieldFilter f)
    {
        var slot = ResolveSlotIndices([f.Member])[0];
        var extractor = SlotAccessor.BuildExtractor(_locator.RecordType!, [slot]);
        var cmp = new DataComparer(f.FieldType);

        switch (f.Op)
        {
            case FilterOp.Equal:
                return rec => cmp.Compare(extractor(rec), f.Value!) == 0;
            case FilterOp.AtLeast:
                return rec => cmp.Compare(extractor(rec), f.Value!) >= 0;
            case FilterOp.GreaterThan:
                return rec => cmp.Compare(extractor(rec), f.Value!) > 0;
            case FilterOp.AtMost:
                return rec => cmp.Compare(extractor(rec), f.Value!) <= 0;
            case FilterOp.LessThan:
                return rec => cmp.Compare(extractor(rec), f.Value!) < 0;
            case FilterOp.Between:
                return rec =>
                {
                    var v = extractor(rec);
                    var lo = cmp.Compare(v, f.Value!);
                    if (lo < 0 || (lo == 0 && !f.FromInclusive)) return false;
                    var hi = cmp.Compare(v, f.Value2!);
                    return hi < 0 || (hi == 0 && f.ToInclusive);
                };
            case FilterOp.Prefix:
                var prefix = ReadString(f.Value!);
                return rec => ReadString(extractor(rec))?.StartsWith(prefix ?? string.Empty, StringComparison.Ordinal) == true;
            default:
                throw new NotSupportedException($"Operator {f.Op} not supported in residual.");
        }
    }

    // ── Ordering ───────────────────────────────────────────────────────────────

    private Comparison<KeyValuePair<IData, IData>> BuildRowComparer(List<SortField> sorts)
    {
        if (sorts.Count == 0)
        {
            var keyCmp = _locator.KeyComparer!;
            return (a, b) => keyCmp.Compare(a.Key, b.Key);
        }

        var steps = sorts.Select(s =>
        {
            if (s.Member is null)
            {
                var keyCmp = _locator.KeyComparer!;
                return (Compare: (Func<KeyValuePair<IData, IData>, KeyValuePair<IData, IData>, int>)
                    ((a, b) => keyCmp.Compare(a.Key, b.Key)), s.Descending);
            }
            var slot = ResolveSlotIndices([s.Member])[0];
            var extractor = SlotAccessor.BuildExtractor(_locator.RecordType!, [slot]);
            var cmp = new DataComparer(s.FieldType!);
            return (Compare: (Func<KeyValuePair<IData, IData>, KeyValuePair<IData, IData>, int>)
                ((a, b) => cmp.Compare(extractor(a.Value), extractor(b.Value))), s.Descending);
        }).ToArray();

        // Tie-break by primary key, following the lead sort key's direction (so an all-descending
        // sort yields pk-descending ties, matching a backward index scan).
        var keyTieDescending = sorts[0].Descending;
        return (a, b) =>
        {
            foreach (var (compare, descending) in steps)
            {
                var c = compare(a, b);
                if (c != 0) return descending ? -c : c;
            }
            var k = _locator.KeyComparer!.Compare(a.Key, b.Key);
            return keyTieDescending ? -k : k;
        };
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    /// <summary>Reads the boxed <c>Value</c> of a primitive <c>Data&lt;T&gt;</c> as string (or null).</summary>
    private static string? ReadString(IData data)
        => data.GetType().GetField("Value")?.GetValue(data) as string;

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
