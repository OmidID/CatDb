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

public class IndexFindCommand : ICommand
{
    public string IndexName;
    public IData FieldValue;
    public List<KeyValuePair<IData, IData>>? Results;

    public IndexFindCommand(string indexName, IData fieldValue)
    {
        IndexName = indexName;
        FieldValue = fieldValue;
    }

    public int Code => CommandCode.INDEX_FIND;
    public bool IsSynchronous => true;
}

public class IndexFindRangeCommand : ICommand
{
    public string IndexName;
    public IData? From;
    public bool HasFrom;
    public IData? To;
    public bool HasTo;
    public List<KeyValuePair<IData, IData>>? Results;

    public IndexFindRangeCommand(string indexName, IData? from, bool hasFrom, IData? to, bool hasTo)
    {
        IndexName = indexName;
        From = from;
        HasFrom = hasFrom;
        To = to;
        HasTo = hasTo;
    }

    public int Code => CommandCode.INDEX_FIND_RANGE;
    public bool IsSynchronous => true;
}

public class IndexExistsCommand : ICommand
{
    public string IndexName;
    public IData FieldValue;
    public bool Result;

    public IndexExistsCommand(string indexName, IData fieldValue)
    {
        IndexName = indexName;
        FieldValue = fieldValue;
    }

    public int Code => CommandCode.INDEX_EXISTS;
    public bool IsSynchronous => true;
}

public class IndexCountCommand : ICommand
{
    public string IndexName;
    public IData FieldValue;
    public long Result;

    public IndexCountCommand(string indexName, IData fieldValue)
    {
        IndexName = indexName;
        FieldValue = fieldValue;
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
