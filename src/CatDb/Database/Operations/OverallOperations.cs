// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database.Operations;

public abstract class OverallOperation(int action) : IOperation
{
    public int            Code    { get; } = action;
    public OperationScope Scope   => OperationScope.Overall;
    public IData          FromKey => null;
    public IData          ToKey   => null;
}

public class ClearOperation() : OverallOperation(OperationCode.CLEAR)
{
}
