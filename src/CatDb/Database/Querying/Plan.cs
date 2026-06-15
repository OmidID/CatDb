// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;

namespace CatDb.Database.Querying;

using Row = KeyValuePair<IData, IData>;

/// <summary>Compiled, value-independent predicate over a record or primary key (reads literals from
/// <see cref="IQueryEngineContext.Parameter"/>) — lets the whole plan be cached and reused per shape.</summary>
internal delegate bool RowPredicate(IData value, IQueryEngineContext ctx);

/// <summary>
/// Physical plan node (Volcano/iterator model). The compiler builds a tree of these once per query
/// shape; the executor pulls rows lazily so memory stays bounded and <c>Take</c> short-circuits.
/// </summary>
public abstract class PlanNode
{
    public abstract string Explain(int indent = 0);
    protected static string Pad(int indent) => new(' ', indent * 2);
}

// ── Primary-key–producing operators ──────────────────────────────────────────

internal abstract class PkPlan : PlanNode
{
    public abstract IEnumerable<IData> Execute(IQueryEngineContext ctx);
}

internal sealed class IndexEqualSeek(string indexName, int valueSlot, string member) : PkPlan
{
    public override IEnumerable<IData> Execute(IQueryEngineContext ctx)
        => ctx.SeekEqual(indexName, ctx.Parameter(valueSlot));
    public override string Explain(int indent) => $"{Pad(indent)}IndexEqualSeek({member} == ?, idx={indexName})";
}

internal sealed class IndexRangeSeek(
    string indexName, string member,
    int fromSlot, bool hasFrom, bool fromInclusive,
    int toSlot, bool hasTo, bool toInclusive, bool backward) : PkPlan
{
    public override IEnumerable<IData> Execute(IQueryEngineContext ctx)
        => ctx.SeekRange(indexName,
            hasFrom ? ctx.Parameter(fromSlot) : null, hasFrom, fromInclusive,
            hasTo ? ctx.Parameter(toSlot) : null, hasTo, toInclusive, backward);
    public override string Explain(int indent) => $"{Pad(indent)}IndexRangeSeek({member}, idx={indexName})";
}

/// <summary>Streams every primary key of an index in field order (forward = ascending, backward =
/// descending). Used to serve ORDER BY straight from an index — no buffering, no Sort node.</summary>
internal sealed class IndexOrderedScan(string indexName, bool backward) : PkPlan
{
    public override IEnumerable<IData> Execute(IQueryEngineContext ctx)
        => ctx.SeekRange(indexName, null, false, true, null, false, true, backward);
    public override string Explain(int indent)
        => $"{Pad(indent)}IndexOrderedScan(idx={indexName}{(backward ? ", backward" : "")})";
}

/// <summary>AND of pk streams — smaller (more selective) sides hashed, the largest streamed.</summary>
internal sealed class IntersectPk(IReadOnlyList<PkPlan> inputs) : PkPlan
{
    public override IEnumerable<IData> Execute(IQueryEngineContext ctx)
        => inputs.Count == 1 ? inputs[0].Execute(ctx) : Intersect(ctx);

    private IEnumerable<IData> Intersect(IQueryEngineContext ctx)
    {
        var eq = ctx.KeyEqualityComparer;
        var probes = new HashSet<IData>[inputs.Count - 1];
        for (var i = 0; i < inputs.Count - 1; i++)
            probes[i] = new HashSet<IData>(inputs[i].Execute(ctx), eq);

        foreach (var p in probes)
            if (p.Count == 0) yield break;

        foreach (var pk in inputs[^1].Execute(ctx))
        {
            var all = true;
            foreach (var p in probes)
                if (!p.Contains(pk)) { all = false; break; }
            if (all) yield return pk;
        }
    }

    public override string Explain(int indent)
        => $"{Pad(indent)}Intersect (AND)\n" + string.Join("\n", inputs.Select(i => i.Explain(indent + 1)));
}

/// <summary>OR of pk streams, de-duplicated.</summary>
internal sealed class UnionPk(IReadOnlyList<PkPlan> inputs) : PkPlan
{
    public override IEnumerable<IData> Execute(IQueryEngineContext ctx)
    {
        var seen = new HashSet<IData>(ctx.KeyEqualityComparer);
        foreach (var input in inputs)
            foreach (var pk in input.Execute(ctx))
                if (seen.Add(pk)) yield return pk;
    }

    public override string Explain(int indent)
        => $"{Pad(indent)}Union (OR)\n" + string.Join("\n", inputs.Select(i => i.Explain(indent + 1)));
}

// ── Row-producing operators ──────────────────────────────────────────────────

internal abstract class RowPlan : PlanNode
{
    public abstract IEnumerable<Row> Execute(IQueryEngineContext ctx);

    /// <summary>Count rows WITHOUT materializing records, when the shape allows it; returns null when a
    /// count requires actually producing rows (e.g. a residual predicate over the record). Lets
    /// <c>Query(...).Count()</c> avoid one main-table heap point-lookup PER matching row — the difference
    /// between O(matches) random reads and a pure index-key count on a large table.</summary>
    public virtual long? CountFast(IQueryEngineContext ctx) => null;

    protected static long Clamp(long n, int skip, int? take)
    {
        var afterSkip = Math.Max(0, n - skip);
        return take.HasValue ? Math.Min(afterSkip, take.Value) : afterSkip;
    }
}

internal sealed class Fetch(PkPlan source) : RowPlan
{
    public override IEnumerable<Row> Execute(IQueryEngineContext ctx)
    {
        foreach (var pk in source.Execute(ctx))
            if (ctx.TryFetch(pk, out var record))
                yield return new Row(pk, record);
    }

    // Count the primary keys the index produced — no per-row TryFetch. Index maintenance keeps index
    // entries in step with the records under the table locks, so the pk count equals the row count
    // (same semantics as the engine's native CountByIndex, which also counts keys without fetching).
    public override long? CountFast(IQueryEngineContext ctx)
    {
        long n = 0;
        foreach (var _ in source.Execute(ctx)) n++;
        return n;
    }

    public override string Explain(int indent) => $"{Pad(indent)}Fetch\n{source.Explain(indent + 1)}";
}

internal sealed class TableScan(
    int fromSlot, bool hasFrom, int toSlot, bool hasTo, bool backward) : RowPlan
{
    public bool HasBounds => hasFrom || hasTo;
    public override IEnumerable<Row> Execute(IQueryEngineContext ctx)
        => ctx.ScanRows(
            hasFrom ? ctx.Parameter(fromSlot) : null, hasFrom,
            hasTo ? ctx.Parameter(toSlot) : null, hasTo, backward);
    public override string Explain(int indent)
        => $"{Pad(indent)}{(HasBounds ? "KeyRangeScan" : "TableScan")}{(backward ? " (backward)" : "")}";
}

internal sealed class Filter(RowPlan input, RowPredicate? recordPredicate, RowPredicate? keyPredicate) : RowPlan
{
    public override IEnumerable<Row> Execute(IQueryEngineContext ctx)
    {
        foreach (var row in input.Execute(ctx))
        {
            if (keyPredicate != null && !keyPredicate(row.Key, ctx)) continue;
            if (recordPredicate != null && !recordPredicate(row.Value, ctx)) continue;
            yield return row;
        }
    }

    // Only safe to count without materializing when there is NO residual predicate — both a record
    // predicate (needs the record) and a key predicate (needs each row's key, which only Fetch produces)
    // force row production. Otherwise defer to the child.
    public override long? CountFast(IQueryEngineContext ctx)
        => (recordPredicate == null && keyPredicate == null) ? input.CountFast(ctx) : null;

    public override string Explain(int indent) => $"{Pad(indent)}Filter\n{input.Explain(indent + 1)}";
}

/// <summary>Order rows. <c>Take</c> ⇒ bounded Top-K max-heap (O(N·log K) time, O(K) memory); else full sort.</summary>
internal sealed class Sort(RowPlan input, Comparison<Row> comparison, int skip, int? take) : RowPlan
{
    public override IEnumerable<Row> Execute(IQueryEngineContext ctx)
        => take.HasValue ? TopK(ctx) : FullSort(ctx);

    // Ordering is irrelevant to a count; defer to the child and apply skip/take.
    public override long? CountFast(IQueryEngineContext ctx)
    {
        var c = input.CountFast(ctx);
        return c.HasValue ? Clamp(c.Value, skip, take) : null;
    }

    private IEnumerable<Row> FullSort(IQueryEngineContext ctx)
    {
        var rows = input.Execute(ctx).ToList();
        rows.Sort(comparison);
        return skip > 0 ? rows.Skip(skip) : rows;
    }

    private IEnumerable<Row> TopK(IQueryEngineContext ctx)
    {
        var k = skip + take!.Value;
        if (k <= 0) return [];

        var heap = new List<Row>(k);
        foreach (var row in input.Execute(ctx))
        {
            if (heap.Count < k) { heap.Add(row); SiftUp(heap, heap.Count - 1); }
            else if (comparison(row, heap[0]) < 0) { heap[0] = row; SiftDown(heap, 0); }
        }

        heap.Sort(comparison);
        return skip > 0 ? heap.Skip(skip) : heap;
    }

    private void SiftUp(List<Row> heap, int i)
    {
        while (i > 0)
        {
            var parent = (i - 1) / 2;
            if (comparison(heap[i], heap[parent]) <= 0) break;
            (heap[i], heap[parent]) = (heap[parent], heap[i]);
            i = parent;
        }
    }

    private void SiftDown(List<Row> heap, int i)
    {
        var n = heap.Count;
        while (true)
        {
            var largest = i;
            var l = 2 * i + 1;
            var r = 2 * i + 2;
            if (l < n && comparison(heap[l], heap[largest]) > 0) largest = l;
            if (r < n && comparison(heap[r], heap[largest]) > 0) largest = r;
            if (largest == i) break;
            (heap[i], heap[largest]) = (heap[largest], heap[i]);
            i = largest;
        }
    }

    public override string Explain(int indent)
        => $"{Pad(indent)}Sort{(take.HasValue ? $" (Top-{skip + take.Value})" : "")}\n{input.Explain(indent + 1)}";
}

internal sealed class Limit(RowPlan input, int skip, int? take) : RowPlan
{
    public override IEnumerable<Row> Execute(IQueryEngineContext ctx)
    {
        IEnumerable<Row> rows = input.Execute(ctx);
        if (skip > 0) rows = rows.Skip(skip);
        if (take.HasValue) rows = rows.Take(take.Value);
        return rows;
    }

    public override long? CountFast(IQueryEngineContext ctx)
    {
        var c = input.CountFast(ctx);
        return c.HasValue ? Clamp(c.Value, skip, take) : null;
    }

    public override string Explain(int indent)
        => $"{Pad(indent)}Limit(skip={skip}, take={(take?.ToString() ?? "all")})\n{input.Explain(indent + 1)}";
}
