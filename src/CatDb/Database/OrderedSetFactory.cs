using CatDb.Data;
using CatDb.General.Collections;
using CatDb.WaterfallTree;

namespace CatDb.Database
{
    public class OrderedSetFactory : IOrderedSetFactory
    {
        public Locator Locator { get; private set; }
        
        public OrderedSetFactory(Locator locator)
        {
            Locator = locator;
        }

        public IOrderedSet<IData, IData> Create()
        {
            var data = new OrderedSet<IData, IData>(Locator.KeyComparer, Locator.KeyEqualityComparer);
            
            return data;
        }
    }
}
