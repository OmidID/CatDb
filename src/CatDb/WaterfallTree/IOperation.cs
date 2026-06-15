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

    /// <summary>
    /// The op-log LSN of the batch this operation belongs to. Stamped at append time (TransactionLog mode),
    /// transient (lives only while the op is buffered; never serialised). The incremental checkpoint uses it
    /// to track each node's oldest-unflushed and max-applied LSN. 0 when not in TransactionLog mode.
    /// </summary>
    long Lsn { get; set; }
}