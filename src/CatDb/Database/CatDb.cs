// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using System.Globalization;
using CatDb.General.Communication;
using CatDb.General.IO;
using CatDb.Remote;
using CatDb.Storage;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public static class CatDb
{
    public static IStorageEngine FromHeap(IHeap heap, DatabaseOptions? options = null) =>
        new StorageEngine(heap, options);

    public static IStorageEngine FromStream(
        Stream stream,
        DatabaseOptions? options = null,
        bool useCompression = false,
        AllocationStrategy allocationStrategy = AllocationStrategy.FromTheBeginning) =>
        FromHeap(new Heap(stream, useCompression, allocationStrategy), options);

    public static IStorageEngine FromMemory(
        DatabaseOptions? options = null,
        bool useCompression = false,
        AllocationStrategy allocationStrategy = AllocationStrategy.FromTheBeginning) =>
        FromStream(new MemoryStream(), options, useCompression, allocationStrategy);

    /// <summary>
    /// Open or create a database from a file.
    /// Default commit mode is WriteAheadLog (crash-safe).
    /// </summary>
    public static IStorageEngine FromFile(
        string fileName,
        DatabaseOptions? options = null,
        bool useCompression = false,
        AllocationStrategy allocationStrategy = AllocationStrategy.FromTheBeginning)
    {
        options ??= DatabaseOptions.Default;
        var stream = new OptimizedFileStream(fileName, FileMode.OpenOrCreate);
        // FromTheBeginning: reuse freed space (lower offsets) before extending the file. Without it the
        // allocator always carves from the giant end-chunk, so every commit's rewritten nodes append and
        // freed slots are never reclaimed — the file grows unbounded (16 GB for ~0.5 GB of live data) and
        // I/O across it slowly decays. Costs some fragmentation; far cheaper than an ever-growing file.
        var heap = new Heap(stream, useCompression, allocationStrategy);

        if (options.CommitMode == CommitMode.WriteAheadLog)
        {
            var walPath = fileName + ".wal";
            var walHeap = new WalHeap(heap, walPath);
            return new StorageEngine(walHeap, options);
        }

        if (options.CommitMode == CommitMode.TransactionLog)
        {
            // Plain heap (atomic header) + an append-only op-log. Commit = log fsync; the background
            // checkpoint flushes nodes to the heap and truncates the log. No WalHeap needed.
            var log = new OperationLog(fileName + ".oplog");
            return new StorageEngine(heap, options, log);
        }

        return new StorageEngine(heap, options);
    }

    public static IStorageEngine FromNetwork(
        string host,
        int port = 7182,
        string databaseName = "default",
        string? userName = null,
        string? password = null,
        RemoteScanOptions? scanOptions = null)
    {
        var client = new StorageEngineClient(host, port, databaseName, userName, password);
        if (scanOptions != null) client.ScanOptions = scanOptions;
        return client;
    }

    /// <summary>Fully async version of <see cref="FromNetwork"/>.</summary>
    public static async Task<IStorageEngine> FromNetworkAsync(
        string host,
        int port = 7182,
        string databaseName = "default",
        string? userName = null,
        string? password = null,
        RemoteScanOptions? scanOptions = null,
        CancellationToken ct = default)
    {
        var client = StorageEngineClient.CreateUnconnected(host, port, databaseName, userName, password);
        if (scanOptions != null) client.ScanOptions = scanOptions;
        await client.ConnectAsync(ct).ConfigureAwait(false);
        return client;
    }

    public static StorageEngineServer CreateServer(IStorageEngine engine, int port = 7182)
    {
        var server       = new TcpServer(port);
        var engineServer = new StorageEngineServer(engine, server);
        return engineServer;
    }

    /// <summary>
    /// Open (or create) a database from a single ADO.NET-style connection string, covering all three
    /// backends: file, in-memory, and remote network. Format is <c>Key=Value;Key=Value;...</c> — keys
    /// are case-insensitive and several common aliases are accepted. <c>Provider</c> selects the backend
    /// explicitly (<c>File</c>/<c>Disk</c>, <c>Memory</c>/<c>Mem</c>/<c>InMemory</c>,
    /// <c>Network</c>/<c>Remote</c>/<c>Tcp</c>/<c>Server</c>); if omitted it's inferred from which other
    /// keys are present (<c>Host</c> → Network, <c>Path</c> → File, else → Memory).
    /// </summary>
    /// <example>
    /// File:    "Provider=File;Path=catdb.db;CommitMode=TransactionLog;CacheSizeBytes=2GB"
    /// Memory:  "Provider=Memory;UseNativeLeafStorage=true"
    /// Network: "Provider=Network;Host=localhost;Port=7182;Database=default;User Id=admin;Password=secret"
    /// </example>
    public static IStorageEngine FromConnectionString(string connectionString)
    {
        var settings = ConnectionStringSettings.Parse(connectionString);

        return settings.Provider switch
        {
            ConnectionProvider.File => CreateFileFromSettings(settings),
            ConnectionProvider.Memory => CreateMemoryFromSettings(settings),
            ConnectionProvider.Network => CreateNetworkFromSettings(settings),
            _ => throw new FormatException($"Unknown provider '{settings.Provider}'.")
        };
    }

    /// <summary>
    /// Fully async version of <see cref="FromConnectionString"/>. Only the Network provider actually
    /// awaits (connect over the wire); File/Memory complete synchronously.
    /// </summary>
    public static async Task<IStorageEngine> FromConnectionStringAsync(string connectionString, CancellationToken ct = default)
    {
        var settings = ConnectionStringSettings.Parse(connectionString);

        return settings.Provider switch
        {
            ConnectionProvider.File => CreateFileFromSettings(settings),
            ConnectionProvider.Memory => CreateMemoryFromSettings(settings),
            ConnectionProvider.Network => await CreateNetworkFromSettingsAsync(settings, ct).ConfigureAwait(false),
            _ => throw new FormatException($"Unknown provider '{settings.Provider}'.")
        };
    }

    private static IStorageEngine CreateFileFromSettings(ConnectionStringSettings settings)
    {
        var path = settings.GetRequired("Path", "File", "Filename", "DataSource", "Data Source");
        var options = settings.BuildDatabaseOptions();
        var useCompression = settings.GetBool("UseCompression", "Compression") ?? false;
        var allocationStrategy = settings.GetEnum<AllocationStrategy>("AllocationStrategy", "Strategy") ?? AllocationStrategy.FromTheBeginning;

        return FromFile(path, options, useCompression, allocationStrategy);
    }

    private static IStorageEngine CreateMemoryFromSettings(ConnectionStringSettings settings)
    {
        var options = settings.BuildDatabaseOptions();
        var useCompression = settings.GetBool("UseCompression", "Compression") ?? false;
        var allocationStrategy = settings.GetEnum<AllocationStrategy>("AllocationStrategy", "Strategy") ?? AllocationStrategy.FromTheBeginning;

        return FromMemory(options, useCompression, allocationStrategy);
    }

    private static IStorageEngine CreateNetworkFromSettings(ConnectionStringSettings settings)
    {
        var (host, port, databaseName, userName, password, scanOptions) = settings.BuildNetworkSettings();
        var client = new StorageEngineClient(host, port, databaseName, userName, password);
        ApplyClientTuning(client, settings, scanOptions);
        return client;
    }

    private static async Task<IStorageEngine> CreateNetworkFromSettingsAsync(ConnectionStringSettings settings, CancellationToken ct)
    {
        var (host, port, databaseName, userName, password, scanOptions) = settings.BuildNetworkSettings();
        var client = StorageEngineClient.CreateUnconnected(host, port, databaseName, userName, password);
        ApplyClientTuning(client, settings, scanOptions);
        await client.ConnectAsync(ct).ConfigureAwait(false);
        return client;
    }

    private static void ApplyClientTuning(StorageEngineClient client, ConnectionStringSettings settings, RemoteScanOptions? scanOptions)
    {
        if (scanOptions != null) client.ScanOptions = scanOptions;

        var writeBatchCapacity = settings.GetInt("WriteBatchCapacity");
        if (writeBatchCapacity.HasValue) client.WriteBatchCapacity = writeBatchCapacity.Value;

        var cacheSize = settings.GetInt("CacheSize", "ClientCacheSize");
        if (cacheSize.HasValue) client.CacheSize = cacheSize.Value;
    }

    private enum ConnectionProvider { File, Memory, Network }

    /// <summary>Parsed <c>Key=Value;...</c> connection string with alias-aware, case-insensitive lookups.</summary>
    private sealed class ConnectionStringSettings
    {
        private static readonly (string Suffix, long Factor)[] ByteSizeSuffixes =
        [
            ("TB", 1024L * 1024 * 1024 * 1024),
            ("GB", 1024L * 1024 * 1024),
            ("MB", 1024L * 1024),
            ("KB", 1024L),
            ("B", 1L)
        ];

        private readonly Dictionary<string, string> _values;

        private ConnectionStringSettings(Dictionary<string, string> values, ConnectionProvider provider)
        {
            _values = values;
            Provider = provider;
        }

        public ConnectionProvider Provider { get; }

        public static ConnectionStringSettings Parse(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Connection string is empty.", nameof(connectionString));

            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var part in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
            {
                var separatorIndex = part.IndexOf('=');
                if (separatorIndex <= 0)
                    throw new FormatException($"Malformed connection string segment '{part}'. Expected Key=Value.");

                var key = part[..separatorIndex].Trim();
                var value = part[(separatorIndex + 1)..].Trim();
                values[key] = value;
            }

            var probe = new ConnectionStringSettings(values, ConnectionProvider.Memory);
            var providerText = probe.GetString("Provider", "Mode", "Type");

            ConnectionProvider provider;
            if (providerText != null)
            {
                provider = providerText.ToLowerInvariant() switch
                {
                    "file" or "disk" => ConnectionProvider.File,
                    "memory" or "mem" or "inmemory" => ConnectionProvider.Memory,
                    "network" or "remote" or "tcp" or "server" => ConnectionProvider.Network,
                    _ => throw new FormatException($"Unknown Provider '{providerText}'. Expected File, Memory, or Network.")
                };
            }
            else if (probe.GetString("Host", "Server", "Address") != null)
                provider = ConnectionProvider.Network;
            else if (probe.GetString("Path", "File", "Filename", "DataSource", "Data Source") != null)
                provider = ConnectionProvider.File;
            else
                provider = ConnectionProvider.Memory;

            return new ConnectionStringSettings(values, provider);
        }

        public string? GetString(params string[] keys)
        {
            foreach (var key in keys)
                if (_values.TryGetValue(key, out var value))
                    return value;

            return null;
        }

        public string GetRequired(params string[] keys) =>
            GetString(keys) ?? throw new ArgumentException($"Connection string is missing required key '{keys[0]}'.");

        public bool? GetBool(params string[] keys)
        {
            var value = GetString(keys);
            if (value == null) return null;

            return value.ToLowerInvariant() switch
            {
                "true" or "1" or "yes" or "on" => true,
                "false" or "0" or "no" or "off" => false,
                _ => throw new FormatException($"Invalid boolean value '{value}' for key '{keys[0]}'.")
            };
        }

        public int? GetInt(params string[] keys)
        {
            var value = GetString(keys);
            if (value == null) return null;

            if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result))
                throw new FormatException($"Invalid integer value '{value}' for key '{keys[0]}'.");

            return result;
        }

        /// <summary>Parses a byte count, optionally suffixed with KB/MB/GB/TB (e.g. "2GB", "512KB").</summary>
        public long? GetByteSize(params string[] keys)
        {
            var value = GetString(keys);
            if (value == null) return null;

            var trimmed = value.Trim();
            var multiplier = 1L;
            foreach (var (suffix, factor) in ByteSizeSuffixes)
            {
                if (!trimmed.EndsWith(suffix, StringComparison.OrdinalIgnoreCase)) continue;

                multiplier = factor;
                trimmed = trimmed[..^suffix.Length].Trim();
                break;
            }

            if (!long.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out var number))
                throw new FormatException($"Invalid byte-size value '{value}' for key '{keys[0]}'.");

            return number * multiplier;
        }

        public TEnum? GetEnum<TEnum>(params string[] keys) where TEnum : struct, Enum
        {
            var value = GetString(keys);
            if (value == null) return null;

            if (!Enum.TryParse<TEnum>(value, ignoreCase: true, out var result))
                throw new FormatException($"Invalid {typeof(TEnum).Name} value '{value}' for key '{keys[0]}'.");

            return result;
        }

        /// <summary>Builds a <see cref="DatabaseOptions"/> from every recognized tuning key (local providers only).</summary>
        public DatabaseOptions BuildDatabaseOptions()
        {
            var options = DatabaseOptions.Default;

            var commitMode = GetEnum<CommitMode>("CommitMode");
            if (commitMode.HasValue) options.CommitMode = commitMode.Value;

            var commitDurability = GetEnum<CommitDurability>("CommitDurability", "Durability");
            if (commitDurability.HasValue) options.CommitDurability = commitDurability.Value;

            var checkpointIntervalMs = GetInt("CheckpointIntervalMs", "CheckpointInterval");
            if (checkpointIntervalMs.HasValue) options.CheckpointIntervalMs = checkpointIntervalMs.Value;

            var checkpointLogSizeBytes = GetByteSize("CheckpointLogSizeBytes", "CheckpointLogSize");
            if (checkpointLogSizeBytes.HasValue) options.CheckpointLogSizeBytes = checkpointLogSizeBytes.Value;

            var incrementalCheckpoint = GetBool("IncrementalCheckpoint");
            if (incrementalCheckpoint.HasValue) options.IncrementalCheckpoint = incrementalCheckpoint.Value;

            var checkpointMaxNodes = GetInt("CheckpointMaxNodes");
            if (checkpointMaxNodes.HasValue) options.CheckpointMaxNodes = checkpointMaxNodes.Value;

            var maxBranchesPerNode = GetInt("MaxBranchesPerNode");
            if (maxBranchesPerNode.HasValue) options.MaxBranchesPerNode = maxBranchesPerNode.Value;

            var maxRecordsPerLeaf = GetInt("MaxRecordsPerLeaf");
            if (maxRecordsPerLeaf.HasValue) options.MaxRecordsPerLeaf = maxRecordsPerLeaf.Value;

            var minRecordsPerLeaf = GetInt("MinRecordsPerLeaf");
            if (minRecordsPerLeaf.HasValue) options.MinRecordsPerLeaf = minRecordsPerLeaf.Value;

            var maxOperationsInRoot = GetInt("MaxOperationsInRoot");
            if (maxOperationsInRoot.HasValue) options.MaxOperationsInRoot = maxOperationsInRoot.Value;

            var maxOperationsPerNode = GetInt("MaxOperationsPerNode");
            if (maxOperationsPerNode.HasValue) options.MaxOperationsPerNode = maxOperationsPerNode.Value;

            var minOperationsPerNode = GetInt("MinOperationsPerNode");
            if (minOperationsPerNode.HasValue) options.MinOperationsPerNode = minOperationsPerNode.Value;

            var cacheSize = GetInt("CacheSize");
            if (cacheSize.HasValue) options.CacheSize = cacheSize.Value;

            var cacheSizeBytes = GetByteSize("CacheSizeBytes");
            if (cacheSizeBytes.HasValue) options.CacheSizeBytes = cacheSizeBytes.Value;

            var useNativeLeafStorage = GetBool("UseNativeLeafStorage", "NativeLeafStorage");
            if (useNativeLeafStorage.HasValue) options.UseNativeLeafStorage = useNativeLeafStorage.Value;

            return options;
        }

        /// <summary>Builds the tuple of connection details needed by <see cref="StorageEngineClient"/> (Network provider only).</summary>
        public (string Host, int Port, string DatabaseName, string? UserName, string? Password, RemoteScanOptions? ScanOptions) BuildNetworkSettings()
        {
            var host = GetRequired("Host", "Server", "Address");
            var port = GetInt("Port") ?? 7182;
            var databaseName = GetString("Database", "DatabaseName", "Catalog") ?? "default";
            var userName = GetString("UserName", "User Id", "UID", "User");
            var password = GetString("Password", "PWD");

            var initialPageCapacity = GetInt("InitialPageCapacity");
            var maxPageCapacity = GetInt("MaxPageCapacity");
            var pageGrowthFactor = GetInt("PageGrowthFactor");

            RemoteScanOptions? scanOptions = null;
            if (initialPageCapacity.HasValue || maxPageCapacity.HasValue || pageGrowthFactor.HasValue)
            {
                scanOptions = new RemoteScanOptions();
                if (initialPageCapacity.HasValue) scanOptions.InitialPageCapacity = initialPageCapacity.Value;
                if (maxPageCapacity.HasValue) scanOptions.MaxPageCapacity = maxPageCapacity.Value;
                if (pageGrowthFactor.HasValue) scanOptions.PageGrowthFactor = pageGrowthFactor.Value;
            }

            return (host, port, databaseName, userName, password, scanOptions);
        }
    }
}