// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using CatDb.Data;
using CatDb.General.Collections;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public class OrderedSetFactory(Locator locator) : IOrderedSetFactory
{
    public Locator Locator { get; } = locator;

    public IOrderedSet<IData, IData> Create() =>
        new OrderedSet<IData, IData>(Locator.KeyComparer, Locator.KeyEqualityComparer);
}
