using System.Diagnostics;
using CatDb.Data;
using CatDb.Database.Operations;
using CatDb.General.Extensions;
using CatDb.WaterfallTree;

namespace CatDb.Database
{
    public class OperationCollection : List<IOperation>, IOperationCollection
    {
        private IOperation[] Array => this.GetArray();

        public OperationCollection(Locator locator, IOperation[] operations, int commonAction, bool areAllMonotoneAndPoint)
        {
            this.SetArray(operations);
            this.SetCount(operations.Length);

            Locator = locator;
            CommonAction = commonAction;
            AreAllMonotoneAndPoint = areAllMonotoneAndPoint;
        }

        public OperationCollection(Locator locator, int capacity)
            : base(capacity)
        {
            Locator = locator;
            CommonAction = OperationCode.UNDEFINED;
            AreAllMonotoneAndPoint = true;
        }

        public new void Add(IOperation operation)
        {
            if (AreAllMonotoneAndPoint)
            {
                if (Count == 0)
                {
                    AreAllMonotoneAndPoint = operation.Scope == OperationScope.Point;
                    CommonAction = operation.Code;
                }
                else
                {
                    if (operation.Scope != OperationScope.Point || Locator.KeyComparer.Compare(operation.FromKey, this[Count - 1].FromKey) <= 0)
                        AreAllMonotoneAndPoint = false;
                }
            }

            if (CommonAction != OperationCode.UNDEFINED && CommonAction != operation.Code)
                CommonAction = OperationCode.UNDEFINED;

            base.Add(operation);
        }

        public void AddRange(IOperationCollection operations)
        {
            if (!operations.AreAllMonotoneAndPoint)
                AreAllMonotoneAndPoint = false;
            else
            {
                if (AreAllMonotoneAndPoint && Count > 0 && operations.Count > 0 && Locator.KeyComparer.Compare(this[Count - 1].FromKey, operations[0].FromKey) >= 0)
                    AreAllMonotoneAndPoint = false;
            }

            if (operations.CommonAction != CommonAction)
            {
                if (Count == 0)
                    CommonAction = operations.CommonAction;
                else if (operations.Count > 0)
                    CommonAction = OperationCode.UNDEFINED;
            }

            var oprs = operations as OperationCollection;

            if (oprs != null)
                this.AddRange(oprs.Array, 0, oprs.Count);
            else
            {
                foreach (var o in operations)
                    Add(o);
            }
        }

        public new void Clear()
        {
            base.Clear();
            CommonAction = OperationCode.UNDEFINED;
            AreAllMonotoneAndPoint = true;
        }

        public IOperationCollection Midlle(int index, int count)
        {
            var array = new IOperation[count];
            System.Array.Copy(Array, index, array, 0, count);

            return new OperationCollection(Locator, array, CommonAction, AreAllMonotoneAndPoint);
        }

        public int BinarySearch(IData key, int index, int count)
        {
            Debug.Assert(AreAllMonotoneAndPoint);

            var low = index;
            var high = index + count - 1;

            var comparer = Locator.KeyComparer;

            while (low <= high)
            {
                var mid = (low + high) >> 1;
                var cmp = comparer.Compare(this[mid].FromKey, key);

                if (cmp == 0)
                    return mid;
                if (cmp < 0)
                    low = mid + 1;
                else
                    high = mid - 1;
            }

            return ~low;
        }

        public int CommonAction { get; private set; }
        public bool AreAllMonotoneAndPoint { get; private set; }

        public Locator Locator { get; private set; }
    }
}
