// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Security.Cryptography;
using System.Text;
using CatDb.Database;

namespace CatDb.Server.Services;

public sealed class SystemCatalogService : IDisposable
{
    public const string SystemDatabaseName = "system.catdb";

    private const string UsersTableName = "__system_users";
    private const string DatabasesTableName = "__system_databases";

    private readonly ILogger<SystemCatalogService> _logger;
    private readonly IStorageEngine _systemEngine;
    private readonly ITable<string, SystemUserRecord> _users;
    private readonly ITable<string, SystemDatabaseRecord> _databases;
    private readonly object _gate = new();

    public SystemCatalogService(string systemDatabasePath, ILogger<SystemCatalogService> logger)
    {
        _logger = logger;
        _systemEngine = Database.CatDb.FromFile(systemDatabasePath);
        _users = _systemEngine.OpenXTablePortable<string, SystemUserRecord>(UsersTableName);
        _databases = _systemEngine.OpenXTablePortable<string, SystemDatabaseRecord>(DatabasesTableName);
    }

    public void EnsureInitialized()
    {
        lock (_gate)
        {
            if (!_databases.Exists(SystemDatabaseName))
            {
                _databases.Replace(SystemDatabaseName, new SystemDatabaseRecord
                {
                    Name = SystemDatabaseName,
                    CreatedAtUtc = DateTime.UtcNow,
                });
            }

            if (_users.Count() == 0)
            {
                const string adminUserName = "admin";
                const string adminPassword = "admin";

                _logger.LogWarning("No users found in system catalog. Bootstrapping default admin user '{UserName}'.", adminUserName);
                var wildcardPermissions = new Dictionary<string, DatabasePermission>
                {
                    ["*"] = DatabasePermission.Admin,
                };

                _users.Replace(adminUserName, new SystemUserRecord
                {
                    UserName = adminUserName,
                    PasswordHash = ComputeHash(adminPassword),
                    GlobalPermissions = SerializeGlobalPermissions(GlobalPermission.Admin | GlobalPermission.ListDatabases | GlobalPermission.ManageDatabases | GlobalPermission.ManageUsers),
                    DatabasePermissions = SerializeDatabasePermissions(wildcardPermissions),
                });
            }

            _systemEngine.Commit();
        }
    }

    public AuthenticatedUser? Authenticate(string? userName, string? password)
    {
        if (string.IsNullOrWhiteSpace(userName) || string.IsNullOrWhiteSpace(password))
            return null;

        lock (_gate)
        {
            if (!_users.TryGet(userName, out var record) || record == null)
                return null;

            var computedHash = ComputeHash(password);
            if (!StringComparer.Ordinal.Equals(record.PasswordHash, computedHash))
                return null;

            return new AuthenticatedUser
            {
                UserName = record.UserName,
                GlobalPermissions = ParseGlobalPermissions(record.GlobalPermissions),
                DatabasePermissions = ParseDatabasePermissions(record.DatabasePermissions),
            };
        }
    }

    public bool DatabaseExists(string databaseName)
    {
        lock (_gate)
            return _databases.Exists(databaseName);
    }

    public void RegisterDatabase(string databaseName)
    {
        lock (_gate)
        {
            _databases.Replace(databaseName, new SystemDatabaseRecord
            {
                Name = databaseName,
                CreatedAtUtc = DateTime.UtcNow,
            });
            _systemEngine.Commit();
        }
    }

    public void DeleteDatabaseRegistration(string databaseName)
    {
        lock (_gate)
        {
            if (_databases.Exists(databaseName))
                _databases.Delete(databaseName);
            _systemEngine.Commit();
        }
    }

    public (IReadOnlyList<SystemDatabaseRecord> Items, long Total) ListDatabases(int page, int pageSize)
    {
        lock (_gate)
        {
            var total = _databases.Count();
            var skip = (page - 1) * pageSize;
            var items = _databases.Forward()
                .Select(kv => kv.Value)
                .Skip(skip)
                .Take(pageSize)
                .ToList();

            return (items, total);
        }
    }

    public (IReadOnlyList<SystemUserView> Items, long Total) ListUsers(int page, int pageSize)
    {
        lock (_gate)
        {
            var total = _users.Count();
            var skip = (page - 1) * pageSize;
            var items = _users.Forward()
                .Select(kv => new SystemUserView
                {
                    UserName = kv.Key,
                    GlobalPermissions = kv.Value.GlobalPermissions,
                    DatabasePermissions = kv.Value.DatabasePermissions,
                })
                .Skip(skip)
                .Take(pageSize)
                .ToList();

            return (items, total);
        }
    }

    public void UpsertUser(
        string userName,
        string password,
        GlobalPermission globalPermissions,
        Dictionary<string, DatabasePermission> databasePermissions)
    {
        lock (_gate)
        {
            _users.Replace(userName, new SystemUserRecord
            {
                UserName = userName,
                PasswordHash = ComputeHash(password),
                GlobalPermissions = SerializeGlobalPermissions(globalPermissions),
                DatabasePermissions = SerializeDatabasePermissions(databasePermissions),
            });
            _systemEngine.Commit();
        }
    }

    public void DeleteUser(string userName)
    {
        lock (_gate)
        {
            if (_users.Exists(userName))
                _users.Delete(userName);
            _systemEngine.Commit();
        }
    }

    public void Dispose()
    {
        _systemEngine.Close();
    }

    private static string ComputeHash(string input)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        var hash = SHA256.HashData(bytes);
        return Convert.ToHexString(hash);
    }

    private static string SerializeGlobalPermissions(GlobalPermission permissions)
    {
        return permissions.ToString();
    }

    private static GlobalPermission ParseGlobalPermissions(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return GlobalPermission.None;

        return Enum.TryParse<GlobalPermission>(value, out var parsed)
            ? parsed
            : GlobalPermission.None;
    }

    private static string SerializeDatabasePermissions(Dictionary<string, DatabasePermission> permissions)
    {
        if (permissions.Count == 0)
            return string.Empty;

        return string.Join(";", permissions.Select(kv => $"{kv.Key}={kv.Value}"));
    }

    private static Dictionary<string, DatabasePermission> ParseDatabasePermissions(string value)
    {
        var result = new Dictionary<string, DatabasePermission>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(value))
            return result;

        foreach (var segment in value.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var idx = segment.IndexOf('=');
            if (idx <= 0 || idx >= segment.Length - 1)
                continue;

            var databaseName = segment[..idx];
            var permissionText = segment[(idx + 1)..];

            if (!Enum.TryParse<DatabasePermission>(permissionText, out var parsed))
                continue;

            result[databaseName] = parsed;
        }

        return result;
    }
}
