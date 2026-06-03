// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.WaterfallTree;

public interface IOperationCollectionFactory
{
    IOperationCollection Create(int capacity);
    IOperationCollection Create(IOperation[] operations, int commonAction, bool areAllMonotoneAndPoint);
}
