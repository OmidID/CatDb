using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database.Operations
{
    public abstract class OverallOperation : IOperation
    {
        public OverallOperation(int action)
        {
            Code = action;
        }

        public int Code { get; private set; }

        public OperationScope Scope => OperationScope.Overall;

        public IData FromKey => null;

        public IData ToKey => null;
    }

    public class ClearOperation : OverallOperation
    {
        public ClearOperation()
            : base(OperationCode.CLEAR)
        {
        }
    }
}
