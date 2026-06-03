// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.General.Persist;
public interface IIndexerPersist
{
}

public interface IIndexerPersist<T> : IIndexerPersist
{
    void Store(BinaryWriter writer, Func<int, T> values, int count);
    void Load(BinaryReader reader, Action<int, T> values, int count);
}
