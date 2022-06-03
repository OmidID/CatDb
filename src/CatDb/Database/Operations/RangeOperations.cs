using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database.Operations
{
    public abstract class RangeOperation : IOperation
    {
        private readonly IData _from;
        private readonly IData _to;

        protected RangeOperation(int action, IData from, IData to)
        {
            Code = action;
            _from = from;
            _to = to;
        }

        protected RangeOperation(int action)
        {
            Code = action;
        }

        public int Code { get; private set; }

        public OperationScope Scope => OperationScope.Range;

        public IData FromKey => _from;

        public IData ToKey => _to;
    }

    public class DeleteRangeOperation : RangeOperation
    {
        public DeleteRangeOperation(IData from, IData to)
            : base(OperationCode.DELETE_RANGE, from, to)
        {
        }
    }
}
