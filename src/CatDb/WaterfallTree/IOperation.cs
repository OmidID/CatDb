// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.Data;

namespace CatDb.WaterfallTree;

public enum OperationScope : byte
{
    Point,
    Range,
    Overall
}

public interface IOperation
{
    int            Code    { get; }
    OperationScope Scope   { get; }
    IData          FromKey { get; }
    IData          ToKey   { get; }
}