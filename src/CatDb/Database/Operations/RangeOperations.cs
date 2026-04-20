using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database.Operations;

public abstract class RangeOperation : IOperation
{
    protected RangeOperation(int action, IData from, IData to)
    {
        Code = action;
        FromKey = from;
        ToKey   = to;
    }

    protected RangeOperation(int action) => Code = action;

    public int            Code    { get; private set; }
    public OperationScope Scope   => OperationScope.Range;
    public IData          FromKey { get; }
    public IData          ToKey   { get; }
}

public class DeleteRangeOperation(IData from, IData to)
    : RangeOperation(OperationCode.DELETE_RANGE, from, to) { }
