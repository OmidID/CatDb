using CatDb.WaterfallTree;

namespace CatDb.Database
{
    public class OperationCollectionFactory : IOperationCollectionFactory
    {
        private readonly Locator _locator;
        
        public OperationCollectionFactory(Locator locator)
        {
            _locator = locator;
        }

        public IOperationCollection Create(int capacity)
        {
            return new OperationCollection(_locator, capacity);
        }

        public IOperationCollection Create(IOperation[] operations, int commonAction, bool areAllMonotoneAndPoint)
        {
            return new OperationCollection(_locator, operations, commonAction, areAllMonotoneAndPoint);
        }
    }
}
