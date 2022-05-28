using CatDb.Data;

namespace CatDb.General.Collections
{
    public interface IOrderedSetFactory
    {
        IOrderedSet<IData, IData> Create();
    }
}
