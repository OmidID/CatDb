using CatDb.Data;
using CatDb.General.Collections;

namespace CatDb.WaterfallTree;

public interface IApply
{
    /// Compact the operations; returns true if the collection was modified.
    bool Internal(IOperationCollection operations);

    bool Leaf(IOperationCollection operations, IOrderedSet<IData, IData> data);

    Locator Locator { get; }
}
