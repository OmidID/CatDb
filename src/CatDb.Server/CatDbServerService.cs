using System.Diagnostics;
using CatDb.Database;
using CatDb.General.Communication;
using CatDb.Remote;

namespace CatDb.Server;

public sealed class CatDbServerService(
    IConfiguration config,
    ILogger<CatDbServerService> logger,
    ServerState state) : BackgroundService
{
    private static readonly ActivitySource ActivitySource = new("CatDb.Server");

    private IStorageEngine? _engine;
    private StorageEngineServer? _server;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var fileName = config["CatDb:FileName"] ?? "catdb.db";
        var port     = config.GetValue<int>("CatDb:Port", 7182);

        using var activity = ActivitySource.StartActivity("CatDbServer.Start");

        logger.LogInformation("Opening database file {File}", fileName);
        _engine = Database.CatDb.FromFile(fileName);

        logger.LogInformation("Starting TCP listener on port {Port}", port);
        var tcpServer = new TcpServer(port);
        _server = new StorageEngineServer(_engine, tcpServer);
        _server.Start();

        state.IsRunning = true;
        state.Port      = port;
        state.FileName  = fileName;

        logger.LogInformation("CatDb server is ready on port {Port}", port);

        await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Stopping CatDb server...");

        state.IsRunning = false;

        _server?.Stop();
        _engine?.Close();

        await base.StopAsync(cancellationToken);

        logger.LogInformation("CatDb server stopped");
    }
}
