using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Remote.Commands
{
    #region ITable Operations

    public class ReplaceCommand : ICommand
    {
        public IData Key;
        public IData Record;

        public ReplaceCommand(IData key, IData record)
        {
            Key = key;
            Record = record;
        }

        public int Code => CommandCode.REPLACE;

        public bool IsSynchronous => false;
    }

    public class DeleteCommand : ICommand
    {
        public IData Key;

        public DeleteCommand(IData key)
        {
            Key = key;
        }

        public int Code => CommandCode.DELETE;

        public bool IsSynchronous => false;
    }

    public class DeleteRangeCommand : ICommand
    {
        public IData FromKey;
        public IData ToKey;

        public DeleteRangeCommand(IData fromKey, IData toKey)
        {
            FromKey = fromKey;
            ToKey = toKey;
        }

        public int Code => CommandCode.DELETE_RANGE;

        public bool IsSynchronous => false;
    }

    public class InsertOrIgnoreCommand : ICommand
    {
        public IData Key;
        public IData Record;

        public InsertOrIgnoreCommand(IData key, IData record)
        {
            Key = key;
            Record = record;
        }

        public int Code => CommandCode.INSERT_OR_IGNORE;

        public bool IsSynchronous => false;
    }

    public class ClearCommand : ICommand
    {
        public int Code => CommandCode.CLEAR;

        public bool IsSynchronous => false;
    }

    public class FirstRowCommand : ICommand
    {
        public KeyValuePair<IData, IData>? Row;

        public FirstRowCommand(KeyValuePair<IData, IData>? row)
        {
            Row = row;
        }

        public FirstRowCommand()
            : this(null)
        {
        }

        public bool IsSynchronous => true;

        public int Code => CommandCode.FIRST_ROW;
    }

    public class LastRowCommand : ICommand
    {
        public KeyValuePair<IData, IData>? Row;

        public LastRowCommand(KeyValuePair<IData, IData>? row)
        {
            Row = row;
        }

        public LastRowCommand()
            : this(null)
        {
        }

        public bool IsSynchronous => true;

        public int Code => CommandCode.LAST_ROW;
    }

    public class CountCommand : ICommand
    {
        public long Count;

        public CountCommand(long count)
        {
            Count = count;
        }

        public CountCommand()
            : this(0)
        {
        }

        public bool IsSynchronous => true;

        public int Code => CommandCode.COUNT;
    }

    public abstract class OutValueCommand : ICommand
    {
        private readonly int _code;

        public IData Key;
        public IData Record;

        public OutValueCommand(int code, IData key, IData record)
        {
            _code = code;

            Key = key;
            Record = record;
        }

        public int Code => _code;

        public bool IsSynchronous => true;
    }

    public class TryGetCommand : OutValueCommand
    {
        public TryGetCommand(IData key, IData record)
            : base(CommandCode.TRY_GET, key, record)
        {
        }

        public TryGetCommand(IData key)
            : this(key, null)
        {
        }
    }

    public abstract class OutKeyValueCommand : ICommand
    {
        private readonly int _code;

        public IData Key;
        public KeyValuePair<IData, IData>? KeyValue;

        public OutKeyValueCommand(int code, IData key, KeyValuePair<IData, IData>? keyValue)
        {
            _code = code;

            Key = key;
            KeyValue = keyValue;
        }

        public int Code => _code;

        public bool IsSynchronous => true;
    }

    public class FindNextCommand : OutKeyValueCommand
    {
        public FindNextCommand(IData key, KeyValuePair<IData, IData>? keyValue)
            : base(CommandCode.FIND_NEXT, key, keyValue)
        {
        }

        public FindNextCommand(IData key)
            : this(key, null)
        {
        }
    }

    public class FindAfterCommand : OutKeyValueCommand
    {
        public FindAfterCommand(IData key, KeyValuePair<IData, IData>? keyValue)
            : base(CommandCode.FIND_AFTER, key, keyValue)
        {
        }

        public FindAfterCommand(IData key)
            : this(key, null)
        {
        }
    }

    public class FindPrevCommand : OutKeyValueCommand
    {
        public FindPrevCommand(IData key, KeyValuePair<IData, IData>? keyValue)
            : base(CommandCode.FIND_PREV, key, keyValue)
        {
        }

        public FindPrevCommand(IData key)
            : this(key, null)
        {
        }
    }

    public class FindBeforeCommand : OutKeyValueCommand
    {
        public FindBeforeCommand(IData key, KeyValuePair<IData, IData>? keyValue)
            : base(CommandCode.FIND_BEFORE, key, keyValue)
        {
        }

        public FindBeforeCommand(IData key)
            : this(key, null)
        {
        }
    }

    #endregion

    #region IteratorOperations

    public abstract class IteratorCommand : ICommand
    {
        private readonly int _code;

        public IData FromKey;
        public IData ToKey;

        public int PageCount;
        public List<KeyValuePair<IData, IData>> List;

        public IteratorCommand(int code, int pageCount, IData from, IData to, List<KeyValuePair<IData, IData>> list)
        {
            _code = code;

            FromKey = from;
            ToKey = to;

            PageCount = pageCount;
            List = list;
        }

        public bool IsSynchronous => true;

        public int Code => _code;
    }

    public class ForwardCommand : IteratorCommand
    {
        public ForwardCommand(int pageCount, IData from, IData to, List<KeyValuePair<IData, IData>> list)
            : base(CommandCode.FORWARD, pageCount, from, to, list)
        {
        }
    }

    public class BackwardCommand : IteratorCommand
    {
        public BackwardCommand(int pageCount, IData from, IData to, List<KeyValuePair<IData, IData>> list)
            : base(CommandCode.BACKWARD, pageCount, from, to, list)
        {
        }
    }

    #endregion

    #region Descriptor

    public class XTableDescriptorGetCommand : ICommand
    {
        public IDescriptor Descriptor;

        public XTableDescriptorGetCommand(IDescriptor descriptor)
        {
            Descriptor = descriptor;
        }

        public int Code => CommandCode.XTABLE_DESCRIPTOR_GET;

        public bool IsSynchronous => true;
    }

    public class XTableDescriptorSetCommand : ICommand
    {
        public IDescriptor Descriptor;

        public XTableDescriptorSetCommand(IDescriptor descriptor)
        {
            Descriptor = descriptor;
        }

        public int Code => CommandCode.XTABLE_DESCRIPTOR_SET;

        public bool IsSynchronous => true;
    }

    #endregion
}
