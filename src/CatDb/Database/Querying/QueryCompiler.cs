// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Text;
using CatDb.Data;

namespace CatDb.Database.Querying;

using Row = KeyValuePair<IData, IData>;

/// <summary>A cached, parameterized physical plan: the operator tree plus how many literal slots it
/// expects. The literals themselves are supplied per execution (see <see cref="QueryCompiler.Collect"/>).</summary>
internal sealed record CompiledPlan(RowPlan Plan, int ParamCount);

/// <summary>
/// Compiles a logical <see cref="EngineQuery"/> into a streaming, <b>parameterized</b> physical plan
/// (literals referenced by slot, so one plan is reused for every value combination of the same shape),
/// and provides the structural <see cref="Signature"/> used as the plan-cache key plus the per-execution
/// value vector via <see cref="Collect"/>. Index seeks are chosen per predicate; ANDed seeks intersect
/// (most-selective first), ORed seeks union when every branch is index-covered (else scan + residual);
/// NOT and non-indexable predicates become an engine-evaluated residual.
/// </summary>
internal static class QueryCompiler
{
    private sealed class Slots { public int Next; }

    // ── Compile (cache miss) ──────────────────────────────────────────────────

    public static CompiledPlan Compile(EngineQuery q, IQueryEngineContext ctx)
    {
        var slots = new Slots();
        var (pk, residual) = CompileNode(q.Filter, ctx, slots);

        var keyFromSlot = q.HasKeyFrom ? slots.Next++ : -1;
        var keyToSlot = q.HasKeyTo ? slots.Next++ : -1;
        var keyPredicate = BuildKeyPredicate(q, keyFromSlot, keyToSlot);

        var sortByKeyOnly = q.Sorts.Count == 0 || (q.Sorts.Count == 1 && q.Sorts[0].Member is null);

        RowPlan rows;
        var streamingOrder = false;
        if (pk is null)
        {
            var descending = q.Sorts.Count == 1 && q.Sorts[0].Descending;
            var backward = sortByKeyOnly && residual is null && descending;
            rows = new TableScan(keyFromSlot, q.HasKeyFrom, keyToSlot, q.HasKeyTo, backward);
            streamingOrder = sortByKeyOnly && residual is null;
        }
        else
        {
            rows = new Fetch(pk);
        }

        if (residual is not null || keyPredicate is not null)
            rows = new Filter(rows, residual, keyPredicate);

        if (q.Sorts.Count > 0 && !streamingOrder)
            rows = new Sort(rows, BuildComparison(q.Sorts, ctx), q.Skip, q.Take);
        else if (q.Skip > 0 || q.Take.HasValue)
            rows = new Limit(rows, q.Skip, q.Take);

        return new CompiledPlan(rows, slots.Next);
    }

    // node → (pk source, residual). Allocates one slot per literal in canonical pre-order.
    private static (PkPlan? Pk, RowPredicate? Residual) CompileNode(FilterNode? node, IQueryEngineContext ctx, Slots slots)
    {
        switch (node)
        {
            case null:
                return (null, null);

            case PredicateNode p:
            {
                var (vSlot, v2Slot) = AllocSlots(p.Filter, slots);
                if (Indexable(p.Filter, ctx))
                    return (BuildSeek(p.Filter, ctx, vSlot, v2Slot), null);
                return (null, LeafPredicate(p.Filter, vSlot, v2Slot, ctx));
            }

            case AndNode a:
            {
                var sources = new List<(PkPlan Plan, long Sel)>();
                RowPredicate? residual = null;
                foreach (var child in a.Children)
                {
                    var (cp, cr) = CompileNode(child, ctx, slots);
                    if (cp is not null) sources.Add((cp, Selectivity(child, ctx)));
                    if (cr is not null) residual = AndPredicate(residual, cr);
                }
                PkPlan? pk = sources.Count == 0 ? null
                    : sources.Count == 1 ? sources[0].Plan
                    : new IntersectPk(sources.OrderBy(s => s.Sel).Select(s => s.Plan).ToList());
                return (pk, residual);
            }

            case OrNode o:
                if (o.Children.All(c => PureIndexable(c, ctx)))
                {
                    var seeks = o.Children.Select(c => CompileNode(c, ctx, slots).Pk!).ToList();
                    return (seeks.Count == 1 ? seeks[0] : new UnionPk(seeks), null);
                }
                return (null, CompileResidual(o, ctx, slots));

            default: // NotNode
                return (null, CompileResidual(node, ctx, slots));
        }
    }

    // Always-residual compile (scan-evaluated), used for OR fallback / NOT. Same slot order as CompileNode.
    private static RowPredicate CompileResidual(FilterNode node, IQueryEngineContext ctx, Slots slots)
    {
        switch (node)
        {
            case PredicateNode p:
            {
                var (vSlot, v2Slot) = AllocSlots(p.Filter, slots);
                return LeafPredicate(p.Filter, vSlot, v2Slot, ctx);
            }
            case AndNode a:
            {
                var parts = a.Children.Select(c => CompileResidual(c, ctx, slots)).ToArray();
                return (rec, c) =>
                {
                    foreach (var part in parts) if (!part(rec, c)) return false;
                    return true;
                };
            }
            case OrNode o:
            {
                var parts = o.Children.Select(c => CompileResidual(c, ctx, slots)).ToArray();
                return (rec, c) =>
                {
                    foreach (var part in parts) if (part(rec, c)) return true;
                    return false;
                };
            }
            case NotNode n:
            {
                var inner = CompileResidual(n.Child, ctx, slots);
                return (rec, c) => !inner(rec, c);
            }
            default:
                throw new NotSupportedException($"Unknown filter node {node.GetType().Name}.");
        }
    }

    private static (int VSlot, int V2Slot) AllocSlots(FieldFilter f, Slots slots)
    {
        var v = slots.Next++;
        var v2 = f.Op == FilterOp.Between ? slots.Next++ : -1;
        return (v, v2);
    }

    private static bool Indexable(FieldFilter f, IQueryEngineContext ctx)
        => f.Op != FilterOp.Prefix && ctx.ResolveIndex(f.Member) is not null;

    private static bool PureIndexable(FilterNode node, IQueryEngineContext ctx) => node switch
    {
        PredicateNode p => Indexable(p.Filter, ctx),
        AndNode a => a.Children.All(c => PureIndexable(c, ctx)),
        OrNode o => o.Children.All(c => PureIndexable(c, ctx)),
        _ => false,
    };

    private static long Selectivity(FilterNode node, IQueryEngineContext ctx)
    {
        if (node is PredicateNode { Filter: { Op: FilterOp.Equal } f })
        {
            var index = ctx.ResolveIndex(f.Member);
            if (index is not null) return ctx.IndexSelectivity(index);
        }
        return long.MaxValue; // ranges / unknown drive the streamed side of the intersect
    }

    private static PkPlan BuildSeek(FieldFilter f, IQueryEngineContext ctx, int vSlot, int v2Slot)
    {
        var index = ctx.ResolveIndex(f.Member)!;
        return f.Op switch
        {
            FilterOp.Equal       => new IndexEqualSeek(index, vSlot, f.Member),
            FilterOp.AtLeast     => new IndexRangeSeek(index, f.Member, vSlot, true, true, -1, false, true, false),
            FilterOp.GreaterThan => new IndexRangeSeek(index, f.Member, vSlot, true, false, -1, false, true, false),
            FilterOp.AtMost      => new IndexRangeSeek(index, f.Member, -1, false, true, vSlot, true, true, false),
            FilterOp.LessThan    => new IndexRangeSeek(index, f.Member, -1, false, true, vSlot, true, false, false),
            FilterOp.Between     => new IndexRangeSeek(index, f.Member, vSlot, true, f.FromInclusive, v2Slot, true, f.ToInclusive, false),
            _ => throw new NotSupportedException($"Operator {f.Op} is not index-resolvable."),
        };
    }

    private static RowPredicate LeafPredicate(FieldFilter f, int vSlot, int v2Slot, IQueryEngineContext ctx)
    {
        var extract = ctx.FieldExtractor(f.Member);
        var cmp = new DataComparer(f.FieldType);
        switch (f.Op)
        {
            case FilterOp.Equal:       return (rec, c) => cmp.Compare(extract(rec), c.Parameter(vSlot)) == 0;
            case FilterOp.AtLeast:     return (rec, c) => cmp.Compare(extract(rec), c.Parameter(vSlot)) >= 0;
            case FilterOp.GreaterThan: return (rec, c) => cmp.Compare(extract(rec), c.Parameter(vSlot)) > 0;
            case FilterOp.AtMost:      return (rec, c) => cmp.Compare(extract(rec), c.Parameter(vSlot)) <= 0;
            case FilterOp.LessThan:    return (rec, c) => cmp.Compare(extract(rec), c.Parameter(vSlot)) < 0;
            case FilterOp.Between:
                var fromIncl = f.FromInclusive; var toIncl = f.ToInclusive;
                return (rec, c) =>
                {
                    var v = extract(rec);
                    var lo = cmp.Compare(v, c.Parameter(vSlot));
                    if (lo < 0 || (lo == 0 && !fromIncl)) return false;
                    var hi = cmp.Compare(v, c.Parameter(v2Slot));
                    return hi < 0 || (hi == 0 && toIncl);
                };
            case FilterOp.Prefix:
                return (rec, c) =>
                {
                    var prefix = ReadString(c.Parameter(vSlot)) ?? string.Empty;
                    return ReadString(extract(rec))?.StartsWith(prefix, StringComparison.Ordinal) == true;
                };
            default:
                throw new NotSupportedException($"Operator {f.Op} not supported in residual.");
        }
    }

    private static RowPredicate AndPredicate(RowPredicate? a, RowPredicate b)
        => a is null ? b : (rec, c) => a(rec, c) && b(rec, c);

    private static RowPredicate? BuildKeyPredicate(EngineQuery q, int keyFromSlot, int keyToSlot)
    {
        if (!q.HasKeyRange) return null;
        bool hasFrom = q.HasKeyFrom, fromIncl = q.KeyFromInclusive, hasTo = q.HasKeyTo, toIncl = q.KeyToInclusive;
        return (pk, c) =>
        {
            var cmp = c.KeyComparer;
            if (hasFrom)
            {
                var cc = cmp.Compare(pk, c.Parameter(keyFromSlot));
                if (cc < 0 || (cc == 0 && !fromIncl)) return false;
            }
            if (hasTo)
            {
                var cc = cmp.Compare(pk, c.Parameter(keyToSlot));
                if (cc > 0 || (cc == 0 && !toIncl)) return false;
            }
            return true;
        };
    }

    private static Comparison<Row> BuildComparison(List<SortField> sorts, IQueryEngineContext ctx)
    {
        var steps = sorts.Select(s =>
        {
            if (s.Member is null)
            {
                var keyCmp = ctx.KeyComparer;
                return (Compare: (Func<Row, Row, int>)((a, b) => keyCmp.Compare(a.Key, b.Key)), s.Descending);
            }
            var extract = ctx.FieldExtractor(s.Member);
            var cmp = new DataComparer(s.FieldType!);
            return (Compare: (Func<Row, Row, int>)((a, b) => cmp.Compare(extract(a.Value), extract(b.Value))), s.Descending);
        }).ToArray();

        var keyTieDescending = sorts[0].Descending;
        var keyComparer = ctx.KeyComparer;
        return (a, b) =>
        {
            foreach (var (compare, descending) in steps)
            {
                var c = compare(a, b);
                if (c != 0) return descending ? -c : c;
            }
            var k = keyComparer.Compare(a.Key, b.Key);
            return keyTieDescending ? -k : k;
        };
    }

    // ── Per-execution value vector (canonical order matches Compile's slot order) ──

    public static IData[] Collect(EngineQuery q)
    {
        var values = new List<IData>();
        CollectNode(q.Filter, values);
        if (q.HasKeyFrom) values.Add(q.KeyFrom!);
        if (q.HasKeyTo) values.Add(q.KeyTo!);
        return values.ToArray();
    }

    private static void CollectNode(FilterNode? node, List<IData> values)
    {
        switch (node)
        {
            case null: return;
            case PredicateNode p:
                if (p.Filter.Value is not null) values.Add(p.Filter.Value);
                if (p.Filter.Op == FilterOp.Between && p.Filter.Value2 is not null) values.Add(p.Filter.Value2);
                break;
            case AndNode a:
                foreach (var c in a.Children) CollectNode(c, values);
                break;
            case OrNode o:
                foreach (var c in o.Children) CollectNode(c, values);
                break;
            case NotNode n:
                CollectNode(n.Child, values);
                break;
        }
    }

    // ── Plan-cache key (structure only, value-independent) ────────────────────

    public static string Signature(EngineQuery q)
    {
        var sb = new StringBuilder();
        SignatureNode(q.Filter, sb);
        sb.Append("|S");
        foreach (var s in q.Sorts) sb.Append(s.Member ?? "@key").Append(s.Descending ? '-' : '+').Append(',');
        sb.Append("|K").Append(q.HasKeyFrom ? (q.KeyFromInclusive ? "[" : "(") : "")
          .Append(q.HasKeyTo ? (q.KeyToInclusive ? "]" : ")") : "");
        sb.Append("|P").Append(q.Skip > 0 ? '1' : '0').Append(q.Take.HasValue ? '1' : '0');
        return sb.ToString();
    }

    private static void SignatureNode(FilterNode? node, StringBuilder sb)
    {
        switch (node)
        {
            case null: sb.Append('_'); break;
            case PredicateNode p:
                sb.Append(p.Filter.Member).Append('#').Append((int)p.Filter.Op);
                if (p.Filter.Op == FilterOp.Between) sb.Append(p.Filter.FromInclusive ? 'i' : 'e').Append(p.Filter.ToInclusive ? 'i' : 'e');
                break;
            case AndNode a:
                sb.Append("AND(");
                foreach (var c in a.Children) { SignatureNode(c, sb); sb.Append(','); }
                sb.Append(')');
                break;
            case OrNode o:
                sb.Append("OR(");
                foreach (var c in o.Children) { SignatureNode(c, sb); sb.Append(','); }
                sb.Append(')');
                break;
            case NotNode n:
                sb.Append("NOT("); SignatureNode(n.Child, sb); sb.Append(')');
                break;
        }
    }

    internal static string? ReadString(IData data)
        => data.GetType().GetField("Value")?.GetValue(data) as string;
}
