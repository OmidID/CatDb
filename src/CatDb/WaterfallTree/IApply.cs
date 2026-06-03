// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.Data;
using CatDb.General.Collections;

namespace CatDb.WaterfallTree;

public interface IApply
{
    /// Compact the operations; returns true if the collection was modified.
    bool Internal(IOperationCollection operations);

    bool Leaf(IOperationCollection operations, IOrderedSet<IData, IData> data);

    Locator Locator { get; }
}
