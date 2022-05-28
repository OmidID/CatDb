using CatDb.Data;
using CatDb.General.Collections;

namespace CatDb.WaterfallTree
{
    public interface IApply
    {
        /// <summary>
        /// Compact the operations and returns true, if the collection was modified.
        /// </summary>
        bool Internal(IOperationCollection operations);

        bool Leaf(IOperationCollection operations, IOrderedSet<IData, IData> data);

        Locator Locator { get; }
    }
}
