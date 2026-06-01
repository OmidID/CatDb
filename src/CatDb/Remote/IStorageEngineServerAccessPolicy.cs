// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.General.Communication;
using CatDb.Remote.Commands;
using CatDb.WaterfallTree;

namespace CatDb.Remote;

public interface IStorageEngineServerAccessPolicy
{
    bool TryResolveStorageEngine(
        ServerConnection connection,
        string? databaseName,
        string? userName,
        string? password,
        out IStorageEngine storageEngine,
        out string? errorMessage);

    bool IsCommandAllowed(
        ServerConnection connection,
        string? databaseName,
        string? userName,
        string? password,
        IDescriptor? descriptor,
        ICommand command,
        out string? errorMessage);
}
