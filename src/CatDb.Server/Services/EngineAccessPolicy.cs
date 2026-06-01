// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database;
using CatDb.General.Communication;
using CatDb.Remote;
using CatDb.Remote.Commands;
using CatDb.WaterfallTree;

namespace CatDb.Server.Services;

public sealed class EngineAccessPolicy(
    SystemCatalogService catalog,
    DatabaseHostService hostService) : IStorageEngineServerAccessPolicy
{
    private readonly SystemCatalogService _catalog = catalog;
    private readonly DatabaseHostService _hostService = hostService;

    public bool TryResolveStorageEngine(
        ServerConnection connection,
        string? databaseName,
        string? userName,
        string? password,
        out IStorageEngine storageEngine,
        out string? errorMessage)
    {
        storageEngine = null!;

        var user = _catalog.Authenticate(userName, password);
        if (user == null)
        {
            errorMessage = "Authentication failed.";
            return false;
        }

        var targetDatabase = string.IsNullOrWhiteSpace(databaseName) ? "default" : databaseName;

        if (!_catalog.DatabaseExists(targetDatabase))
        {
            errorMessage = $"Database '{targetDatabase}' not found.";
            return false;
        }

        if (!user.HasDatabase(targetDatabase, DatabasePermission.Read))
        {
            errorMessage = "Permission denied for target database.";
            return false;
        }

        storageEngine = _hostService.GetOrOpenDatabase(targetDatabase);
        errorMessage = null;
        return true;
    }

    public bool IsCommandAllowed(
        ServerConnection connection,
        string? databaseName,
        string? userName,
        string? password,
        IDescriptor? descriptor,
        ICommand command,
        out string? errorMessage)
    {
        var targetDatabase = string.IsNullOrWhiteSpace(databaseName) ? "default" : databaseName;
        var required = GetRequiredPermission(command.Code);

        if (required == DatabasePermission.None)
        {
            errorMessage = null;
            return true;
        }

        var user = _catalog.Authenticate(userName, password);
        if (user == null)
        {
            errorMessage = "Authentication context missing.";
            return false;
        }

        if (!user.HasDatabase(targetDatabase, required))
        {
            errorMessage = "Permission denied for command.";
            return false;
        }

        errorMessage = null;
        return true;
    }

    private static DatabasePermission GetRequiredPermission(int commandCode)
    {
        return commandCode switch
        {
            CommandCode.REPLACE => DatabasePermission.Write,
            CommandCode.DELETE => DatabasePermission.Write,
            CommandCode.DELETE_RANGE => DatabasePermission.Write,
            CommandCode.INSERT_OR_IGNORE => DatabasePermission.Write,
            CommandCode.CLEAR => DatabasePermission.Write,
            CommandCode.TRY_GET => DatabasePermission.Read,
            CommandCode.FORWARD => DatabasePermission.Read,
            CommandCode.BACKWARD => DatabasePermission.Read,
            CommandCode.FIND_NEXT => DatabasePermission.Read,
            CommandCode.FIND_AFTER => DatabasePermission.Read,
            CommandCode.FIND_PREV => DatabasePermission.Read,
            CommandCode.FIND_BEFORE => DatabasePermission.Read,
            CommandCode.FIRST_ROW => DatabasePermission.Read,
            CommandCode.LAST_ROW => DatabasePermission.Read,
            CommandCode.COUNT => DatabasePermission.Read,
            CommandCode.XTABLE_DESCRIPTOR_GET => DatabasePermission.Read,
            CommandCode.XTABLE_DESCRIPTOR_SET => DatabasePermission.Write,
            CommandCode.STORAGE_ENGINE_COMMIT => DatabasePermission.Write,
            CommandCode.STORAGE_ENGINE_GET_ENUMERATOR => DatabasePermission.Read,
            CommandCode.STORAGE_ENGINE_RENAME => DatabasePermission.TableAdmin,
            CommandCode.STORAGE_ENGINE_EXISTS => DatabasePermission.Read,
            CommandCode.STORAGE_ENGINE_FIND_BY_NAME => DatabasePermission.Read,
            CommandCode.STORAGE_ENGINE_FIND_BY_ID => DatabasePermission.Read,
            CommandCode.STORAGE_ENGINE_OPEN_XTABLE => DatabasePermission.TableAdmin,
            CommandCode.STORAGE_ENGINE_OPEN_XFILE => DatabasePermission.TableAdmin,
            CommandCode.STORAGE_ENGINE_DELETE => DatabasePermission.TableAdmin,
            CommandCode.STORAGE_ENGINE_COUNT => DatabasePermission.Read,
            CommandCode.STORAGE_ENGINE_GET_CACHE_SIZE => DatabasePermission.Read,
            CommandCode.STORAGE_ENGINE_SET_CACHE_SIZE => DatabasePermission.Write,
            CommandCode.HEAP_OBTAIN_NEW_HANDLE => DatabasePermission.HeapAccess,
            CommandCode.HEAP_RELEASE_HANDLE => DatabasePermission.HeapAccess,
            CommandCode.HEAP_EXISTS_HANDLE => DatabasePermission.HeapAccess,
            CommandCode.HEAP_WRITE => DatabasePermission.HeapAccess,
            CommandCode.HEAP_READ => DatabasePermission.HeapAccess,
            CommandCode.HEAP_COMMIT => DatabasePermission.HeapAccess,
            CommandCode.HEAP_CLOSE => DatabasePermission.HeapAccess,
            CommandCode.HEAP_GET_TAG => DatabasePermission.HeapAccess,
            CommandCode.HEAP_SET_TAG => DatabasePermission.HeapAccess,
            CommandCode.HEAP_DATA_SIZE => DatabasePermission.HeapAccess,
            CommandCode.HEAP_SIZE => DatabasePermission.HeapAccess,
            _ => DatabasePermission.None,
        };
    }
}
