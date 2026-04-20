using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace CatDb.General.Communication;

/// <summary>
/// High-performance async TCP server.
/// Accepts connections in a non-blocking async loop; each accepted connection
/// runs its own async send/receive loops via <see cref="ServerConnection.RunAsync"/>.
/// Received packets are dispatched via the <see cref="PacketReceived"/> callback
/// — no polling, no <see cref="System.Collections.Concurrent.BlockingCollection{T}"/>.
/// </summary>
public sealed class TcpServer : IAsyncDisposable
{
    private TcpListener?                _listener;
    private CancellationTokenSource?    _cts;
    private Task?                       _acceptTask;

    public int  Port { get; }
    public long BytesReceived;
    public long BytesSent;

    public readonly ConcurrentQueue<(DateTime At, Exception Ex)> Errors = new();
    public readonly ConcurrentDictionary<ServerConnection, ServerConnection> ServerConnections = new();

    /// <summary>
    /// Invoked for every packet received from any connected client.
    /// The callback runs on a ThreadPool thread (via Task.Run) so it
    /// must not block — use async/await inside.
    /// </summary>
    public Func<ServerConnection, Packet, CancellationToken, Task>? PacketReceived { get; set; }

    public bool IsRunning => _cts is { IsCancellationRequested: false };

    public TcpServer(int port = 7182) => Port = port;

    // ── Start / Stop (async-primary, sync wrappers for compat) ───────────────

    public Task StartAsync(CancellationToken ct = default)
    {
        _cts      = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _listener = new TcpListener(IPAddress.Any, Port);
        _listener.Start();
        _acceptTask = AcceptLoopAsync(_cts.Token);
        return Task.CompletedTask;
    }

    public void Start() => StartAsync().GetAwaiter().GetResult();

    public async Task StopAsync()
    {
        _cts?.Cancel();
        _listener?.Stop();
        foreach (var conn in ServerConnections.Values)
            await conn.StopAsync().ConfigureAwait(false);
        if (_acceptTask is not null)
            await _acceptTask.ConfigureAwait(false);
    }

    public void Stop() => StopAsync().GetAwaiter().GetResult();

    // ── Accept loop ───────────────────────────────────────────────────────────

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var client = await _listener!.AcceptTcpClientAsync(ct).ConfigureAwait(false);
                client.NoDelay = true;

                var conn = new ServerConnection(this, client);
                ServerConnections.TryAdd(conn, conn);

                // Each connection owns its lifecycle — fire-and-forget
                _ = conn.RunAsync(ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)               { LogError(ex); }
        finally                            { _listener?.Stop(); }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    internal void LogError(Exception ex)
    {
        while (Errors.Count > 100) Errors.TryDequeue(out _);
        Errors.Enqueue((DateTime.Now, ex));
    }

    public async ValueTask DisposeAsync() => await StopAsync().ConfigureAwait(false);
}
