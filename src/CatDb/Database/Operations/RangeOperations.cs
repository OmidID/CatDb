// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database.Operations;

public abstract class RangeOperation : IOperation
{
    protected RangeOperation(int action, IData from, IData to)
    {
        Code = action;
        FromKey = from;
        ToKey   = to;
    }

    protected RangeOperation(int action) => Code = action;

    public int            Code    { get; private set; }
    public OperationScope Scope   => OperationScope.Range;
    public IData          FromKey { get; }
    public IData          ToKey   { get; }
}

public class DeleteRangeOperation(IData from, IData to)
    : RangeOperation(OperationCode.DELETE_RANGE, from, to) { }
