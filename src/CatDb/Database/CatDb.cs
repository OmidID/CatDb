// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.General.Communication;
using CatDb.General.IO;
using CatDb.Remote;
using CatDb.Storage;
using CatDb.WaterfallTree;

namespace CatDb.Database;

public static class CatDb
{
    public static IStorageEngine FromHeap(IHeap heap, DatabaseOptions? options = null) =>
        new StorageEngine(heap, options);

    public static IStorageEngine FromStream(Stream stream, DatabaseOptions? options = null) =>
        FromHeap(new Heap(stream, false, AllocationStrategy.FromTheBeginning), options);

    public static IStorageEngine FromMemory(DatabaseOptions? options = null) =>
        FromStream(new MemoryStream(), options);

    /// <summary>
    /// Open or create a database from a file.
    /// Default commit mode is WriteAheadLog (crash-safe).
    /// </summary>
    public static IStorageEngine FromFile(string fileName, DatabaseOptions? options = null)
    {
        options ??= DatabaseOptions.Default;
        var stream = new OptimizedFileStream(fileName, FileMode.OpenOrCreate);
        // FromTheBeginning: reuse freed space (lower offsets) before extending the file. Without it the
        // allocator always carves from the giant end-chunk, so every commit's rewritten nodes append and
        // freed slots are never reclaimed — the file grows unbounded (16 GB for ~0.5 GB of live data) and
        // I/O across it slowly decays. Costs some fragmentation; far cheaper than an ever-growing file.
        var heap = new Heap(stream, false, AllocationStrategy.FromTheBeginning);

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
}