using CatDb.WaterfallTree;
using CatDb.Data;

namespace CatDb.Database.Operations
{
    public abstract class RangeOperation : IOperation
    {
        private readonly IData from;
        private readonly IData to;

        protected RangeOperation(int action, IData from, IData to)
        {
            Code = action;
            this.from = from;
            this.to = to;
        }

        protected RangeOperation(int action)
        {
            Code = action;
        }

        public int Code { get; private set; }

        public OperationScope Scope => OperationScope.Range;

        public IData FromKey => from;

        public IData ToKey => to;
    }

    public class DeleteRangeOperation : RangeOperation
    {
        public DeleteRangeOperation(IData from, IData to)
            : base(OperationCode.DELETE_RANGE, from, to)
        {
        }
    }
}
