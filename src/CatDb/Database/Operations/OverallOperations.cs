using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database.Operations;

public abstract class OverallOperation(int action) : IOperation
{
    public int            Code    { get; } = action;
    public OperationScope Scope   => OperationScope.Overall;
    public IData          FromKey => null;
    public IData          ToKey   => null;
}

public class ClearOperation() : OverallOperation(OperationCode.CLEAR)
{
}
