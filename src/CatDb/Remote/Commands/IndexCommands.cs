// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Data;
using CatDb.Database.Indexing;

namespace CatDb.Remote.Commands;

public class IndexCreateCommand : ICommand
{
    public string IndexName;
    public int[] SlotIndices;
    public string[] MemberNames;
    public IndexType IndexType;

    public IndexCreateCommand(string indexName, int[] slotIndices, string[] memberNames, IndexType indexType)
    {
        IndexName = indexName;
        SlotIndices = slotIndices;
        MemberNames = memberNames;
        IndexType = indexType;
    }

    public int Code => CommandCode.INDEX_CREATE;
    public bool IsSynchronous => true;
}

public class IndexDropCommand : ICommand
{
    public string IndexName;

    public IndexDropCommand(string indexName)
    {
        IndexName = indexName;
    }

    public int Code => CommandCode.INDEX_DROP;
    public bool IsSynchronous => true;
}

// Field values cross the wire as raw bytes (serialized via DataPersist of the field type), since the
// index field type differs from the table's key/record types the connection's persisters know.

public class IndexFindCommand : ICommand
{
    public string IndexName;
    public byte[] FieldValueRaw;
    public List<KeyValuePair<IData, IData>>? Results;

    public IndexFindCommand(string indexName, byte[] fieldValueRaw)
    {
        IndexName = indexName;
        FieldValueRaw = fieldValueRaw;
    }

    public int Code => CommandCode.INDEX_FIND;
    public bool IsSynchronous => true;
}

public class IndexFindRangeCommand : ICommand
{
    public string IndexName;
    public byte[]? FromRaw;
    public bool HasFrom;
    public bool FromInclusive;
    public byte[]? ToRaw;
    public bool HasTo;
    public bool ToInclusive;
    public bool Backward;
    public List<KeyValuePair<IData, IData>>? Results;

    public IndexFindRangeCommand(string indexName,
        byte[]? fromRaw, bool hasFrom, bool fromInclusive,
        byte[]? toRaw, bool hasTo, bool toInclusive, bool backward)
    {
        IndexName = indexName;
        FromRaw = fromRaw;
        HasFrom = hasFrom;
        FromInclusive = fromInclusive;
        ToRaw = toRaw;
        HasTo = hasTo;
        ToInclusive = toInclusive;
        Backward = backward;
    }

    public int Code => CommandCode.INDEX_FIND_RANGE;
    public bool IsSynchronous => true;
}

public class IndexFindPrefixCommand : ICommand
{
    public string IndexName;
    public byte[] PrefixRaw;
    public int PrefixFieldCount;
    public bool Backward;
    public List<KeyValuePair<IData, IData>>? Results;

    public IndexFindPrefixCommand(string indexName, byte[] prefixRaw, int prefixFieldCount, bool backward)
    {
        IndexName = indexName;
        PrefixRaw = prefixRaw;
        PrefixFieldCount = prefixFieldCount;
        Backward = backward;
    }

    public int Code => CommandCode.INDEX_FIND_PREFIX;
    public bool IsSynchronous => true;
}

public class IndexExistsCommand : ICommand
{
    public string IndexName;
    public byte[] FieldValueRaw;
    public bool Result;

    public IndexExistsCommand(string indexName, byte[] fieldValueRaw)
    {
        IndexName = indexName;
        FieldValueRaw = fieldValueRaw;
    }

    public int Code => CommandCode.INDEX_EXISTS;
    public bool IsSynchronous => true;
}

public class IndexCountCommand : ICommand
{
    public string IndexName;
    public byte[] FieldValueRaw;
    public long Result;

    public IndexCountCommand(string indexName, byte[] fieldValueRaw)
    {
        IndexName = indexName;
        FieldValueRaw = fieldValueRaw;
    }

    public int Code => CommandCode.INDEX_COUNT;
    public bool IsSynchronous => true;
}

public class IndexRebuildCommand : ICommand
{
    public string? IndexName; // null means rebuild all

    public IndexRebuildCommand(string? indexName)
    {
        IndexName = indexName;
    }

    public int Code => CommandCode.INDEX_REBUILD;
    public bool IsSynchronous => true;
}

public class IndexListCommand : ICommand
{
    public List<IndexDefinition>? Results;

    public int Code => CommandCode.INDEX_LIST;
    public bool IsSynchronous => true;
}

// ── Structured engine query over the wire ─────────────────────────────────────
// Field values are opaque raw bytes (RemoteFieldCodec). The server resolves each field's type from
// its member name, decodes the values, rebuilds the EngineQuery and runs it on its local engine.

/// <summary>Wire form of a <c>FilterNode</c>: Kind 0=predicate, 1=And, 2=Or, 3=Not.</summary>
public sealed class WireNode
{
    public byte Kind;
    // Predicate (Kind 0):
    public string? Member;
    public byte Op;
    public bool FromInclusive;
    public bool ToInclusive;
    public byte[]? ValueRaw;
    public byte[]? Value2Raw;
    // And/Or (Kind 1/2):
    public List<WireNode>? Children;
    // Not (Kind 3):
    public WireNode? Child;
}

public struct WireSort
{
    public string? Member;      // null => primary key
    public bool Descending;
}

public class IndexQueryCommand : ICommand
{
    public WireNode? FilterRoot;
    public List<WireSort> Sorts;

    public bool HasKeyFrom;
    public bool KeyFromInclusive;
    public byte[]? KeyFromRaw;
    public bool HasKeyTo;
    public bool KeyToInclusive;
    public byte[]? KeyToRaw;

    public int Skip;
    public bool HasTake;
    public int Take;

    public List<KeyValuePair<IData, IData>>? Results;

    public IndexQueryCommand(WireNode? filterRoot, List<WireSort> sorts)
    {
        FilterRoot = filterRoot;
        Sorts = sorts;
    }

    public int Code => CommandCode.INDEX_QUERY;
    public bool IsSynchronous => true;
}

/// <summary>
/// Count-only counterpart of <see cref="IndexQueryCommand"/>: same query shape over the wire, but the
/// server returns a single <see cref="Result"/> count instead of materialized rows. Without this, a remote
/// <c>Query(...).Count()</c> had no server-side fast-count path to dispatch to (the local-only fast path
/// checks <c>is TableIndexManager</c>, never true for the remote manager) and fell back to enumerating —
/// transferring every matching FULL RECORD over the wire just to discard it and return a count. The server
/// runs the same <c>TableIndexManager.TryCountFast</c> used locally (index-key-only counting, no per-row
/// heap fetch) and this command carries back only the long.
/// </summary>
public class IndexCountQueryCommand : ICommand
{
    public WireNode? FilterRoot;
    public List<WireSort> Sorts;

    public bool HasKeyFrom;
    public bool KeyFromInclusive;
    public byte[]? KeyFromRaw;
    public bool HasKeyTo;
    public bool KeyToInclusive;
    public byte[]? KeyToRaw;

    public int Skip;
    public bool HasTake;
    public int Take;

    public long Result;

    public IndexCountQueryCommand(WireNode? filterRoot, List<WireSort> sorts)
    {
        FilterRoot = filterRoot;
        Sorts = sorts;
    }

    public int Code => CommandCode.INDEX_COUNT_QUERY;
    public bool IsSynchronous => true;
}
