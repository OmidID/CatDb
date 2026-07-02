// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections.Concurrent;
using CatDb.Database;
using CatDb.Storage;

namespace CatDb.Server.Services;

public sealed class DatabaseHostService : IDisposable
{
    private readonly string _databaseDirectory;
    private readonly ILogger<DatabaseHostService> _logger;
    private readonly SystemCatalogService _catalog;
    private readonly ConcurrentDictionary<string, IStorageEngine> _engines = new(StringComparer.OrdinalIgnoreCase);

    public string DatabaseDirectory => _databaseDirectory;

    public DatabaseHostService(
        string databaseDirectory,
        ILogger<DatabaseHostService> logger,
        SystemCatalogService catalog)
    {
        _databaseDirectory = databaseDirectory;
        _logger = logger;
        _catalog = catalog;

        Directory.CreateDirectory(_databaseDirectory);
    }

    public IStorageEngine GetOrOpenDatabase(string databaseName)
    {
        if (!_catalog.DatabaseExists(databaseName))
            throw new InvalidOperationException($"Database '{databaseName}' is not registered.");

        return _engines.GetOrAdd(databaseName, static (name, state) =>
        {
            var (service, logger) = state;
            var filePath = service.GetDatabasePath(name);
            logger.LogInformation("Opening database {DatabaseName} from {Path}", name, filePath);
            return Database.CatDb.FromFile(filePath, new()
            {
                CommitMode = CommitMode.TransactionLog,
                IncrementalCheckpoint = true,
                UseNativeLeafStorage = true  
            });
        }, (this, _logger));
    }

    public void CreateDatabase(string databaseName)
    {
        if (string.Equals(databaseName, SystemCatalogService.SystemDatabaseName, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Reserved database name.");

        if (_catalog.DatabaseExists(databaseName))
            throw new InvalidOperationException($"Database '{databaseName}' already exists.");

        var filePath = GetDatabasePath(databaseName);
        if (File.Exists(filePath))
            throw new InvalidOperationException($"Database file already exists at '{filePath}'.");

        using (var engine = Database.CatDb.FromFile(filePath))
        {
            engine.Commit();
        }

        _catalog.RegisterDatabase(databaseName);
    }

    public void DeleteDatabase(string databaseName)
    {
        if (string.Equals(databaseName, SystemCatalogService.SystemDatabaseName, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Reserved database cannot be deleted.");

        if (!_catalog.DatabaseExists(databaseName))
            throw new InvalidOperationException($"Database '{databaseName}' does not exist.");

        if (_engines.TryRemove(databaseName, out var engine))
            engine.Close();

        var dbPath = GetDatabasePath(databaseName);
        var walPath = dbPath + ".wal";

        if (File.Exists(dbPath))
            File.Delete(dbPath);

        if (File.Exists(walPath))
            File.Delete(walPath);

        _catalog.DeleteDatabaseRegistration(databaseName);
    }

    public (IReadOnlyList<SystemDatabaseRecord> Items, long Total) ListDatabases(int page, int pageSize)
    {
        return _catalog.ListDatabases(page, pageSize);
    }

    public string GetDatabasePath(string databaseName)
    {
        var safeName = databaseName.Trim();
        if (!safeName.EndsWith(".catdb", StringComparison.OrdinalIgnoreCase))
            safeName += ".catdb";

        return Path.Combine(_databaseDirectory, safeName);
    }

    public void Dispose()
    {
        foreach (var engine in _engines.Values)
            engine.Close();

        _engines.Clear();
    }
}
