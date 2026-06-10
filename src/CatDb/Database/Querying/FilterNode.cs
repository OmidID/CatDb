// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace CatDb.Database.Querying;

/// <summary>
/// Logical filter predicate tree — the boolean expression the planner compiles into a physical plan.
/// Leaves are <see cref="PredicateNode"/> (a single field comparison); internal nodes combine with
/// AND / OR / NOT. This is the engine's representation of a <c>WHERE</c> clause and supports arbitrary
/// nesting/grouping, e.g. <c>(City = 'nyc' OR City = 'la') AND Age &gt;= 30</c>.
/// </summary>
public abstract class FilterNode
{
    /// <summary>AND of the given children (empty list = always-true).</summary>
    public static FilterNode All(IReadOnlyList<FilterNode> children) => new AndNode(children);

    /// <summary>OR of the given children (empty list = always-false).</summary>
    public static FilterNode Any(IReadOnlyList<FilterNode> children) => new OrNode(children);

    public static FilterNode And(FilterNode a, FilterNode b) => new AndNode([a, b]);
    public static FilterNode Or(FilterNode a, FilterNode b) => new OrNode([a, b]);
    public static FilterNode Not(FilterNode child) => new NotNode(child);
    public static FilterNode Leaf(FieldFilter filter) => new PredicateNode(filter);
}

/// <summary>A single field comparison (the leaf of the tree).</summary>
public sealed class PredicateNode(FieldFilter filter) : FilterNode
{
    public FieldFilter Filter { get; } = filter;
}

/// <summary>Conjunction — all children must match.</summary>
public sealed class AndNode(IReadOnlyList<FilterNode> children) : FilterNode
{
    public IReadOnlyList<FilterNode> Children { get; } = children;
}

/// <summary>Disjunction — any child may match.</summary>
public sealed class OrNode(IReadOnlyList<FilterNode> children) : FilterNode
{
    public IReadOnlyList<FilterNode> Children { get; } = children;
}

/// <summary>Negation — the child must not match.</summary>
public sealed class NotNode(FilterNode child) : FilterNode
{
    public FilterNode Child { get; } = child;
}
