﻿using CatDb.Data;

namespace CatDb.WaterfallTree
{
    public interface IOperationCollection : IEnumerable<IOperation>
    {
        void Add(IOperation operation);
        void AddRange(IOperationCollection operations);
        void Clear();

        IOperation this[int index] { get; }
        int Count { get; }
        int Capacity { get; }

        IOperationCollection Midlle(int index, int count);
        int BinarySearch(IData key, int index, int count);

        int CommonAction { get; }
        bool AreAllMonotoneAndPoint { get; }
        
        Locator Locator { get; }
    }
}
