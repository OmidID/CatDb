// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
﻿using CatDb.Data;
using CatDb.WaterfallTree;

namespace CatDb.Database.Operations;

public abstract class PointOperation(int action, IData key) : IOperation
{
    public int            Code    { get; } = action;
    public OperationScope Scope   => OperationScope.Point;
    public IData          FromKey => key;
    public IData          ToKey   => key;
    public long           Lsn     { get; set; }
    public override string ToString() => ToKey.ToString();
}

public class DeleteOperation(IData key) : PointOperation(OperationCode.DELETE, key) { }

public abstract class ValueOperation(int action, IData key, IData record) : PointOperation(action, key)
{
    public IData Record = record;
}

public class ReplaceOperation(IData key, IData record)
    : ValueOperation(OperationCode.REPLACE, key, record) { }

public class InsertOrIgnoreOperation(IData key, IData record)
    : ValueOperation(OperationCode.INSERT_OR_IGNORE, key, record) { }
