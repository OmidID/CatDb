// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.Data;

namespace CatDb.WaterfallTree;

public interface IOperationCollection : IEnumerable<IOperation>
{
    void Add(IOperation operation);
    void AddRange(IOperationCollection operations);
    void AddRange(IOperationCollection operations, int startIndex, int count);
    void Clear();

    IOperation this[int index] { get; }
    int Count    { get; }
    int Capacity { get; }

    IOperationCollection Midlle(int index, int count);
    int BinarySearch(IData key, int index, int count);

    int  CommonAction           { get; }
    bool AreAllMonotoneAndPoint { get; }
    Locator Locator             { get; }
}
