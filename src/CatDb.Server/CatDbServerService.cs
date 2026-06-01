// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Diagnostics;
using CatDb.General.Communication;
using CatDb.Remote;
using CatDb.Server.Services;

namespace CatDb.Server;

public sealed class CatDbServerService(
    IConfiguration config,
    ILogger<CatDbServerService> logger,
    ServerState state,
    SystemCatalogService systemCatalog,
    DatabaseHostService databaseHostService,
    EngineAccessPolicy accessPolicy) : BackgroundService
{
    private static readonly ActivitySource ActivitySource = new("CatDb.Server");

    private StorageEngineServer? _server;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var defaultDatabase = config["CatDb:DefaultDatabase"] ?? "default";
        var port     = config.GetValue<int>("CatDb:Port", 7182);

        using var activity = ActivitySource.StartActivity("CatDbServer.Start");

        systemCatalog.EnsureInitialized();

        if (!systemCatalog.DatabaseExists(defaultDatabase))
            databaseHostService.CreateDatabase(defaultDatabase);

        var defaultEngine = databaseHostService.GetOrOpenDatabase(defaultDatabase);

        logger.LogInformation("Starting TCP listener on port {Port}", port);
        var tcpServer = new TcpServer(port);
        _server = new StorageEngineServer(defaultEngine, tcpServer, accessPolicy);
        await _server.StartAsync(stoppingToken).ConfigureAwait(false);

        state.IsRunning = true;
        state.Port      = port;
        state.DatabaseDirectory = databaseHostService.DatabaseDirectory;
        state.DefaultDatabaseName = defaultDatabase;

        logger.LogInformation("CatDb server is ready on port {Port}", port);

        await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Stopping CatDb server...");

        state.IsRunning = false;

        if (_server is not null) await _server.StopAsync().ConfigureAwait(false);

        await base.StopAsync(cancellationToken);

        logger.LogInformation("CatDb server stopped");
    }
}
