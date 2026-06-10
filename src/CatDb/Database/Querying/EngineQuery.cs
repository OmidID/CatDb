// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;

namespace CatDb.Database.Querying;

/// <summary>Comparison operator for a single structured field predicate.</summary>
public enum FilterOp
{
    Equal,
    AtLeast,      // field >= value          (inclusive lower bound)
    GreaterThan,  // field >  value          (exclusive lower bound)
    AtMost,       // field <= value          (inclusive upper bound)
    LessThan,     // field <  value          (exclusive upper bound)
    Between,      // value <= field <= value2 (inclusivity per flags)
    Prefix,       // string/byte[] field starts with value
}

/// <summary>
/// A structured predicate on a single record field — the engine analyses this (field name,
/// operator, value) to choose an index scan or an engine-evaluated residual. This is what makes
/// filtering happen <b>inside the engine</b> rather than as an opaque caller delegate.
/// </summary>
public sealed class FieldFilter
{
    public required string Member { get; init; }
    public required FilterOp Op { get; init; }
    public required Type FieldType { get; init; }

    /// <summary>Primary comparison value (lower bound for ranges / the prefix / the equal value).</summary>
    public IData? Value { get; init; }
    /// <summary>Upper bound value for <see cref="FilterOp.Between"/>.</summary>
    public IData? Value2 { get; init; }

    public bool FromInclusive { get; init; } = true;
    public bool ToInclusive { get; init; } = true;
}

/// <summary>One ORDER BY key — a record field, or the primary key when <see cref="Member"/> is null.</summary>
public sealed class SortField
{
    public string? Member { get; init; }     // null => primary key
    public required Type? FieldType { get; init; }
    public bool Descending { get; init; }
}

/// <summary>
/// A complete structured query the engine plans and executes itself: a boolean predicate tree
/// (<see cref="Filter"/>), ORDER BY keys, an optional primary-key range, and Skip/Take. Produced by
/// the fluent builder; compiled by <c>QueryPlanner</c> into a streaming physical plan.
/// </summary>
public sealed class EngineQuery
{
    /// <summary>Root of the WHERE predicate tree (null = no field predicate).</summary>
    public FilterNode? Filter { get; set; }
    public List<SortField> Sorts { get; } = [];
    public int Skip { get; set; }
    public int? Take { get; set; }

    // Optional primary-key range filter (engine key scan / residual on pk).
    public IData? KeyFrom { get; set; }
    public bool HasKeyFrom { get; set; }
    public bool KeyFromInclusive { get; set; } = true;
    public IData? KeyTo { get; set; }
    public bool HasKeyTo { get; set; }
    public bool KeyToInclusive { get; set; } = true;

    public bool HasKeyRange => HasKeyFrom || HasKeyTo;
}
