// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text.Json;
using CatDb.Database.Querying;
using Microsoft.AspNetCore.Http;

namespace CatDb.Server.Services;

// ─────────────────────────────────────────────────────────────────────────────
//  HTTP query language for the data list/query endpoints.
//
//  Two front-ends produce the SAME neutral <see cref="ParsedQuery"/> (a filter
//  tree + order + paging + optional key range + count flag), which the engine
//  layer (DataExplorerService) turns into a CatDb.Database.Querying.EngineQuery:
//
//   1. Query string (GET .../{table}?...)  — flat, PostgREST-inspired, colon-delimited:
//        ?City=nyc                     City = "nyc"          (bare value ⇒ eq)
//        ?City=eq:nyc                  City = "nyc"
//        ?Age=gte:30&Age=lt:65         Age >= 30 AND Age < 65   (repeat ⇒ AND)
//        ?Name=prefix:ada              Name starts with "ada"
//        ?Age=between:30:65            30 <= Age <= 65
//        ?or=(City:eq:nyc,City:eq:la)  (City=nyc OR City=la)    (one OR group, AND-ed in)
//        ?order=Age:desc,Name          ORDER BY Age DESC, Name ASC   (also -Age shorthand)
//        ?order=$key:desc              ORDER BY primary key DESC
//        ?limit=20&offset=40           take 20, skip 40   (take/skip also accepted)
//        ?count=true                   include the total match count (fast, no row fetch)
//        ?fromKey=100&toKey=200        primary-key range (combines with filters)
//      Control params (order/limit/offset/skip/take/count/or/fromKey/toKey/
//      direction/fromInclusive/toInclusive) are reserved; every other param is a field predicate.
//
//   2. JSON body (POST .../{table}/query) — full recursive filter tree, no nesting limit:
//        { "filter": { "and": [ {"field":"City","op":"eq","value":"nyc"},
//                               {"or": [ {"field":"Age","op":"lt","value":10},
//                                        {"field":"Age","op":"gt","value":90} ] } ] },
//          "order": [ {"field":"Age","desc":true}, {"field":"$key"} ],
//          "keyFrom": 100, "keyTo": 200, "skip": 0, "take": 20, "count": true }
//      A filter node is a predicate (has "field") OR a combinator ("and"/"or": [nodes] | "not": node).
// ─────────────────────────────────────────────────────────────────────────────

/// <summary>A scalar value not yet bound to a CLR type — carries the raw string (query string) or
/// JSON element (request body); <see cref="Bind"/> converts to the target field/key type on demand.</summary>
public readonly struct RawScalar
{
    private readonly string? _str;
    private readonly JsonElement? _json;

    private RawScalar(string? str, JsonElement? json) { _str = str; _json = json; }
    public static RawScalar FromString(string s) => new(s, null);
    public static RawScalar FromJson(JsonElement j) => new(null, j);

    public object? Bind(Type type)
        => _json is { } j ? ScalarConvert.FromJson(j, type)
         : _str  is { } s ? ScalarConvert.FromString(s, type)
         : null;
}

public abstract class QueryNode;

public sealed class PredicateSpec : QueryNode
{
    public required string Field { get; init; }
    public required FilterOp Op { get; init; }
    public RawScalar Value { get; init; }
    public RawScalar? Value2 { get; init; }              // Between upper bound
    public bool FromInclusive { get; init; } = true;
    public bool ToInclusive { get; init; } = true;
}

public sealed class GroupSpec : QueryNode
{
    public bool IsOr { get; init; }                       // false ⇒ AND
    public List<QueryNode> Nodes { get; init; } = [];
}

public sealed class NotSpec : QueryNode
{
    public required QueryNode Node { get; init; }
}

public sealed class SortSpec
{
    public string? Field { get; init; }                   // null / "$key" / "key" ⇒ primary key
    public bool Descending { get; init; }
}

public sealed class ParsedQuery
{
    public QueryNode? Filter { get; set; }
    public List<SortSpec> Order { get; } = [];
    public int Skip { get; set; }
    public int? Take { get; set; }

    public RawScalar? KeyFrom { get; set; }
    public bool KeyFromInclusive { get; set; } = true;
    public RawScalar? KeyTo { get; set; }
    public bool KeyToInclusive { get; set; } = true;

    public bool Count { get; set; }

    /// <summary>True when nothing beyond an optional primary-key range/direction was requested, so the
    /// fast key-scan browse path can serve it (no need to invoke the query planner).</summary>
    public bool IsPlainBrowse => Filter is null && Order.Count == 0 && !Count;

    /// <summary>Set from the legacy <c>direction=backward</c> when no explicit ORDER BY is given.</summary>
    public bool DefaultKeyDescending { get; set; }
}

/// <summary>Maps HTTP/JSON operator tokens to <see cref="FilterOp"/> and parses scalars.</summary>
public static class QueryOps
{
    public static FilterOp Parse(string token) => token.ToLowerInvariant() switch
    {
        "eq" or "="                     => FilterOp.Equal,
        "gte" or "ge" or ">="           => FilterOp.AtLeast,
        "gt" or ">"                     => FilterOp.GreaterThan,
        "lte" or "le" or "<="           => FilterOp.AtMost,
        "lt" or "<"                     => FilterOp.LessThan,
        "between" or "btw"              => FilterOp.Between,
        "prefix" or "startswith" or "sw" => FilterOp.Prefix,
        _ => throw new ArgumentException($"Unknown filter operator '{token}'. " +
                 "Use one of: eq, gt, gte, lt, lte, between, prefix.")
    };

    /// <summary>True when <paramref name="token"/> is a recognized operator keyword.</summary>
    public static bool IsOp(string token) => token.ToLowerInvariant() is
        "eq" or "=" or "gte" or "ge" or ">=" or "gt" or ">" or "lte" or "le" or "<="
        or "lt" or "<" or "between" or "btw" or "prefix" or "startswith" or "sw";
}

internal static class ScalarConvert
{
    public static object FromString(string s, Type t)
    {
        if (t == typeof(string))   return s;
        if (t == typeof(bool))     return bool.Parse(s);
        if (t == typeof(DateTime)) return DateTime.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        if (t == typeof(byte[]))   return Convert.FromBase64String(s);
        if (t == typeof(Guid))     return Guid.Parse(s);
        return Convert.ChangeType(s, t, CultureInfo.InvariantCulture);
    }

    public static object FromJson(JsonElement el, Type t)
    {
        if (t == typeof(string))   return el.ValueKind == JsonValueKind.String ? el.GetString()! : el.GetRawText();
        if (t == typeof(long))     return el.GetInt64();
        if (t == typeof(int))      return el.GetInt32();
        if (t == typeof(short))    return el.GetInt16();
        if (t == typeof(byte))     return el.GetByte();
        if (t == typeof(sbyte))    return el.GetSByte();
        if (t == typeof(uint))     return el.GetUInt32();
        if (t == typeof(ushort))   return el.GetUInt16();
        if (t == typeof(ulong))    return el.GetUInt64();
        if (t == typeof(double))   return el.GetDouble();
        if (t == typeof(float))    return el.GetSingle();
        if (t == typeof(decimal))  return el.GetDecimal();
        if (t == typeof(bool))     return el.GetBoolean();
        if (t == typeof(char))     return el.GetString()![0];
        if (t == typeof(DateTime)) return el.GetDateTime();
        if (t == typeof(Guid))     return el.GetGuid();
        if (t == typeof(byte[]))   return Convert.FromBase64String(el.GetString() ?? string.Empty);
        // fall back to raw text for anything unusual
        return Convert.ChangeType(el.GetString() ?? el.GetRawText(), t, CultureInfo.InvariantCulture);
    }
}

/// <summary>Reserved query-string params that are NOT field predicates.</summary>
public static class QueryStringParser
{
    private static readonly HashSet<string> Reserved = new(StringComparer.OrdinalIgnoreCase)
    {
        "order", "orderBy", "sort", "limit", "offset", "skip", "take",
        "count", "or", "fromKey", "toKey", "direction",
        "fromInclusive", "toInclusive", "fromExclusive", "toExclusive",
    };

    public static ParsedQuery Parse(IQueryCollection query)
    {
        var q = new ParsedQuery();
        var andNodes = new List<QueryNode>();

        foreach (var kv in query)
        {
            var key = kv.Key;
            if (Reserved.Contains(key)) continue;
            foreach (var raw in kv.Value)                 // repeated field ⇒ multiple AND predicates
            {
                if (raw is null) continue;
                andNodes.Add(ParsePredicate(key, raw));
            }
        }

        // Optional OR group: or=(field:op:value,field:op:value,...)
        var orRaw = query["or"].ToString();
        if (!string.IsNullOrWhiteSpace(orRaw))
        {
            var body = orRaw.Trim();
            if (body.StartsWith('(') && body.EndsWith(')'))
                body = body[1..^1];
            var orNodes = new List<QueryNode>();
            foreach (var triple in SplitTopLevel(body, ','))
            {
                var parts = triple.Split(':', 3);          // field : op : value(:value2)
                if (parts.Length < 2)
                    throw new ArgumentException($"Malformed OR term '{triple}'. Expected field:op:value.");
                var field = parts[0].Trim();
                var op = QueryOps.Parse(parts[1].Trim());
                orNodes.Add(BuildPredicate(field, op, parts.Length > 2 ? parts[2] : ""));
            }
            if (orNodes.Count > 0)
                andNodes.Add(new GroupSpec { IsOr = true, Nodes = orNodes });
        }

        q.Filter = andNodes.Count switch
        {
            0 => null,
            1 => andNodes[0],
            _ => new GroupSpec { IsOr = false, Nodes = andNodes },
        };

        // order=field[:asc|:desc][,-field,...]   (leading '-' ⇒ desc shorthand)
        var order = FirstNonEmpty(query, "order", "orderBy", "sort");
        if (order is not null)
            foreach (var term in order.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                q.Order.Add(ParseSort(term));

        // paging
        q.Take = ParseInt(FirstNonEmpty(query, "limit", "take"));
        q.Skip = ParseInt(FirstNonEmpty(query, "offset", "skip")) ?? 0;

        q.Count = ParseBool(query["count"].ToString());

        // key range (reuses the browse params; combines with filters via the query engine)
        var fromKey = query["fromKey"].ToString();
        var toKey = query["toKey"].ToString();
        if (!string.IsNullOrEmpty(fromKey)) q.KeyFrom = RawScalar.FromString(fromKey);
        if (!string.IsNullOrEmpty(toKey)) q.KeyTo = RawScalar.FromString(toKey);
        q.KeyFromInclusive = !ParseBool(query["fromExclusive"].ToString());
        q.KeyToInclusive = !ParseBool(query["toExclusive"].ToString());

        // legacy direction=backward, only meaningful with no explicit order
        if (string.Equals(query["direction"].ToString(), "backward", StringComparison.OrdinalIgnoreCase)
            && q.Order.Count == 0)
            q.DefaultKeyDescending = true;

        return q;
    }

    private static PredicateSpec ParsePredicate(string field, string rawValue)
    {
        // "<op>:<value>" when the prefix before the first ':' is a known operator; else the whole
        // string is an eq value (so ?City=nyc and ?City=eq:nyc are both accepted, and a value that
        // merely contains a colon — e.g. a name "John:Doe" — is treated as eq, not an operator).
        var colon = rawValue.IndexOf(':');
        if (colon > 0 && QueryOps.IsOp(rawValue[..colon]))
        {
            var op = QueryOps.Parse(rawValue[..colon]);
            return BuildPredicate(field, op, rawValue[(colon + 1)..]);
        }
        return BuildPredicate(field, FilterOp.Equal, rawValue);
    }

    private static PredicateSpec BuildPredicate(string field, FilterOp op, string operand)
    {
        if (op == FilterOp.Between)
        {
            var bounds = operand.Split(':', 2);
            if (bounds.Length != 2)
                throw new ArgumentException($"'between' on '{field}' needs two values: {field}=between:lo:hi.");
            return new PredicateSpec
            {
                Field = field, Op = FilterOp.Between,
                Value = RawScalar.FromString(bounds[0]),
                Value2 = RawScalar.FromString(bounds[1]),
            };
        }
        return new PredicateSpec { Field = field, Op = op, Value = RawScalar.FromString(operand) };
    }

    private static SortSpec ParseSort(string term)
    {
        bool desc = false;
        if (term.StartsWith('-')) { desc = true; term = term[1..]; }
        var colon = term.IndexOf(':');
        if (colon >= 0)
        {
            var dir = term[(colon + 1)..].Trim();
            desc = string.Equals(dir, "desc", StringComparison.OrdinalIgnoreCase);
            term = term[..colon];
        }
        term = term.Trim();
        var field = term is "$key" or "key" or "" ? null : term;
        return new SortSpec { Field = field, Descending = desc };
    }

    // Splits on a separator ignoring separators inside parentheses (so a future nested group survives).
    private static IEnumerable<string> SplitTopLevel(string s, char sep)
    {
        var depth = 0; var start = 0;
        for (var i = 0; i < s.Length; i++)
        {
            var c = s[i];
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == sep && depth == 0) { yield return s[start..i].Trim(); start = i + 1; }
        }
        if (start <= s.Length) yield return s[start..].Trim();
    }

    private static string? FirstNonEmpty(IQueryCollection q, params string[] keys)
    {
        foreach (var k in keys)
        {
            var v = q[k].ToString();
            if (!string.IsNullOrEmpty(v)) return v;
        }
        return null;
    }

    private static int? ParseInt(string? s) => int.TryParse(s, out var n) ? n : null;

    private static bool ParseBool(string? s) =>
        !string.IsNullOrEmpty(s) && (s == "1" || s.Equals("true", StringComparison.OrdinalIgnoreCase));
}

/// <summary>Parses the POST body's recursive filter tree.</summary>
public static class JsonQueryParser
{
    public static ParsedQuery Parse(JsonElement body)
    {
        var q = new ParsedQuery();

        if (body.TryGetProperty("filter", out var filter) && filter.ValueKind == JsonValueKind.Object)
            q.Filter = ParseNode(filter);

        if (body.TryGetProperty("order", out var order) && order.ValueKind == JsonValueKind.Array)
            foreach (var s in order.EnumerateArray())
                q.Order.Add(ParseSort(s));

        if (body.TryGetProperty("skip", out var skip) && skip.ValueKind == JsonValueKind.Number)
            q.Skip = skip.GetInt32();
        if (body.TryGetProperty("take", out var take) && take.ValueKind == JsonValueKind.Number)
            q.Take = take.GetInt32();
        if (body.TryGetProperty("limit", out var limit) && limit.ValueKind == JsonValueKind.Number)
            q.Take = limit.GetInt32();
        if (body.TryGetProperty("offset", out var offset) && offset.ValueKind == JsonValueKind.Number)
            q.Skip = offset.GetInt32();

        if (body.TryGetProperty("count", out var count))
            q.Count = count.ValueKind == JsonValueKind.True;

        if (body.TryGetProperty("keyFrom", out var kf) && kf.ValueKind != JsonValueKind.Null)
            q.KeyFrom = RawScalar.FromJson(kf);
        if (body.TryGetProperty("keyTo", out var kt) && kt.ValueKind != JsonValueKind.Null)
            q.KeyTo = RawScalar.FromJson(kt);
        if (body.TryGetProperty("keyFromInclusive", out var kfi))
            q.KeyFromInclusive = kfi.ValueKind != JsonValueKind.False;
        if (body.TryGetProperty("keyToInclusive", out var kti))
            q.KeyToInclusive = kti.ValueKind != JsonValueKind.False;

        return q;
    }

    private static QueryNode ParseNode(JsonElement node)
    {
        if (node.TryGetProperty("and", out var and) && and.ValueKind == JsonValueKind.Array)
            return new GroupSpec { IsOr = false, Nodes = and.EnumerateArray().Select(ParseNode).ToList() };
        if (node.TryGetProperty("or", out var or) && or.ValueKind == JsonValueKind.Array)
            return new GroupSpec { IsOr = true, Nodes = or.EnumerateArray().Select(ParseNode).ToList() };
        if (node.TryGetProperty("not", out var not) && not.ValueKind == JsonValueKind.Object)
            return new NotSpec { Node = ParseNode(not) };

        if (!node.TryGetProperty("field", out var fieldEl))
            throw new ArgumentException("Filter node must be a predicate ({field,op,value}) or a combinator ({and|or:[…]} / {not:{…}}).");

        var field = fieldEl.GetString() ?? throw new ArgumentException("Predicate 'field' must be a string.");
        var op = node.TryGetProperty("op", out var opEl) ? QueryOps.Parse(opEl.GetString() ?? "eq") : FilterOp.Equal;

        var pred = new PredicateSpec
        {
            Field = field,
            Op = op,
            Value = node.TryGetProperty("value", out var v) ? RawScalar.FromJson(v) : default,
            Value2 = op == FilterOp.Between && node.TryGetProperty("value2", out var v2) ? RawScalar.FromJson(v2) : null,
            FromInclusive = !node.TryGetProperty("fromInclusive", out var fi) || fi.ValueKind != JsonValueKind.False,
            ToInclusive = !node.TryGetProperty("toInclusive", out var ti) || ti.ValueKind != JsonValueKind.False,
        };
        if (op == FilterOp.Between && pred.Value2 is null)
            throw new ArgumentException($"'between' on '{field}' requires both 'value' and 'value2'.");
        return pred;
    }

    private static SortSpec ParseSort(JsonElement s)
    {
        if (s.ValueKind == JsonValueKind.String)
        {
            var f = s.GetString();
            return new SortSpec { Field = f is "$key" or "key" or "" ? null : f, Descending = false };
        }
        var field = s.TryGetProperty("field", out var fe) ? fe.GetString() : null;
        var desc = s.TryGetProperty("desc", out var de) ? de.ValueKind == JsonValueKind.True
                 : s.TryGetProperty("descending", out var d2) && d2.ValueKind == JsonValueKind.True;
        return new SortSpec { Field = field is "$key" or "key" or "" ? null : field, Descending = desc };
    }
}
