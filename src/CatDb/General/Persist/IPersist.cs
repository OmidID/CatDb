// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.General.Persist;
public interface IPersist
{
}

public interface IPersist<T> : IPersist
{
    void Write(BinaryWriter writer, T item);
    T Read(BinaryReader reader);
}
