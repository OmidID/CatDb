using CatDb.WaterfallTree;
using CatDb.Data;

namespace CatDb.Database.Operations
{
    public abstract class PointOperation : IOperation
    {
        private readonly IData key;

        protected PointOperation(int action, IData key)
        {
            Code = action;
            this.key = key;
        }

        public int Code { get; private set; }

        public OperationScope Scope => OperationScope.Point;

        public IData FromKey => key;

        public IData ToKey => key;

        public override string ToString()
        {
            return ToKey.ToString();
        }
    }

    public class DeleteOperation : PointOperation
    {
        public DeleteOperation(IData key)
            : base(OperationCode.DELETE, key)
        {
        }
    }

    public abstract class ValueOperation : PointOperation
    {
        public IData Record;

        public ValueOperation(int action, IData key, IData record)
            : base(action, key)
        {
            Record = record;
        }
    }

    public class ReplaceOperation : ValueOperation
    {
        public ReplaceOperation(IData key, IData record)
            : base(OperationCode.REPLACE, key, record)
        {
        }
    }

    public class InsertOrIgnoreOperation : ValueOperation
    {
        public InsertOrIgnoreOperation(IData key, IData record)
            : base(OperationCode.INSERT_OR_IGNORE, key, record)
        {
        }
    }
}
