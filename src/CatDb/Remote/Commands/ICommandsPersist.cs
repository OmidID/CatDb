// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.Remote.Commands;
public interface ICommandCollectionPersist
{
    void Write(BinaryWriter writer, CommandCollection collection);
    CommandCollection Read(BinaryReader reader);
}
