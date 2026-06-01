// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.Data;

namespace CatDb.General.Collections;
public interface IOrderedSetFactory
{
    IOrderedSet<IData, IData> Create();
}
