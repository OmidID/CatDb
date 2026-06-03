// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.Data;
using CatDb.General.Persist;

namespace CatDb.WaterfallTree;

public class SentinelPersistKey : IPersist<IData>
{
    public static readonly SentinelPersistKey Instance = new();

    public void Write(BinaryWriter writer, IData item) { }

    public IData Read(BinaryReader reader) => null!;
}
