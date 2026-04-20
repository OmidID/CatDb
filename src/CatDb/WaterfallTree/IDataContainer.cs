using CatDb.Data;
using CatDb.General.Collections;

namespace CatDb.WaterfallTree;

public interface IDataContainer : IOrderedSet<IData, IData>
{
    double FillPercentage { get; }
    bool   IsEmpty        { get; }

    /// Excludes and returns the right half of the ordered set.
    IDataContainer Split(double percentage);

    /// Merges the specified set into this set. All keys from one set are less/greater than all keys from the other.
    void Merge(IDataContainer data);

    IData FirstKey { get; }
    IData LastKey  { get; }
}
