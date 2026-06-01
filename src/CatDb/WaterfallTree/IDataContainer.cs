// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.Data;
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
