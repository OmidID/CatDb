using CatDb.WaterfallTree;

namespace CatDb.Database;

public class OperationCollectionFactory(Locator locator) : IOperationCollectionFactory
{
    private readonly Locator _locator = locator;

    public IOperationCollection Create(int capacity) =>
        new OperationCollection(_locator, capacity);

    public IOperationCollection Create(IOperation[] operations, int commonAction, bool areAllMonotoneAndPoint) =>
        new OperationCollection(_locator, operations, commonAction, areAllMonotoneAndPoint);
}
