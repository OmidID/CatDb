using CatDb.Data;

namespace CatDb.WaterfallTree
{
    public enum OperationScope : byte
    {
        Point,
        Range,
        Overall
    }

    public interface IOperation
    {
        int Code { get; }
        OperationScope Scope { get; }

        IData FromKey { get; }
        IData ToKey { get; }
    }
}