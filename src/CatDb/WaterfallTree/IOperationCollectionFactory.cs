namespace CatDb.WaterfallTree
{
    public interface IOperationCollectionFactory
    {
        IOperationCollection Create(int capacity);
        IOperationCollection Create(IOperation[] operations, int commonAction, bool areAllMonotoneAndPoint);
    }
}
