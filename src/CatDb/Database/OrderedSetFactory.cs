using CatDb.Data;
using CatDb.General.Collections;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public class OrderedSetFactory(Locator locator) : IOrderedSetFactory
{
    public Locator Locator { get; } = locator;

    public IOrderedSet<IData, IData> Create() =>
        new OrderedSet<IData, IData>(Locator.KeyComparer, Locator.KeyEqualityComparer);
}
