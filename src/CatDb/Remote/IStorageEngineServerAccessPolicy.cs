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
