using System.Net.Sockets;
using System.Threading.Channels;

namespace CatDb.General.Communication;

/// <summary>
/// Manages one accepted TCP client connection on the server side.
/// A bounded <see cref="Channel{T}"/> buffers outbound response frames.
/// All I/O is async — no thread ever blocks.
/// </summary>
public sealed class ServerConnection : IAsyncDisposable
{
    private readonly TcpServer _server;
    private readonly TcpClient _client;

    // Send channel carries (id, responsePayload) tuples
    private readonly Channel<(long Id, MemoryStream Data)> _sendChannel =
        Channel.CreateBounded<(long, MemoryStream)>(new BoundedChannelOptions(256)
        {
            FullMode                      = BoundedChannelFullMode.Wait,
            SingleReader                  = true,
            AllowSynchronousContinuations = false,
        });

    private CancellationTokenSource? _cts;

    public ServerConnection(TcpServer server, TcpClient client)
    {
        _server = server ?? throw new ArgumentNullException(nameof(server));
        _client = client ?? throw new ArgumentNullException(nameof(client));
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Runs the receive and send loops until the connection drops or
    /// <paramref name="serverCt"/> is cancelled.
    /// Called (fire-and-forget) by <see cref="TcpServer"/>'s accept loop.
    /// </summary>
    internal async Task RunAsync(CancellationToken serverCt)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(serverCt);
        var stream = _client.GetStream();
        try
        {
            var recv = ReceiveLoopAsync(stream, _cts.Token);
            var send = SendLoopAsync(stream, _cts.Token);

            // When either loop ends (connection drop / cancellation), stop both
            await Task.WhenAny(recv, send).ConfigureAwait(false);
        }
        finally
        {
            _cts.Cancel();
            _sendChannel.Writer.TryComplete();
            _client.Dispose();
            _server.ServerConnections.TryRemove(this, out _);
        }
    }

    // ── Enqueue response ──────────────────────────────────────────────────────

    /// <summary>Enqueues a response frame to be sent back to the client.</summary>
    public ValueTask SendResponseAsync(long id, MemoryStream data, CancellationToken ct = default) =>
        _sendChannel.Writer.WriteAsync((id, data), ct);

    // ── Receive loop ──────────────────────────────────────────────────────────

    private async Task ReceiveLoopAsync(NetworkStream stream, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var (id, ms) = await FrameProtocol.ReadAsync(stream, ct).ConfigureAwait(false);
                Interlocked.Add(ref _server.BytesReceived, ms.Length);

                var packet = new Packet(ms) { Id = id };

                if (_server.PacketReceived is { } handler)
                    _ = Task.Run(() => handler(this, packet, ct), ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)               { _server.LogError(ex); }
    }

    // ── Send loop ─────────────────────────────────────────────────────────────

    private async Task SendLoopAsync(NetworkStream stream, CancellationToken ct)
    {
        try
        {
            await foreach (var (id, data) in _sendChannel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                Interlocked.Add(ref _server.BytesSent, data.Length);
                await FrameProtocol.WriteAsync(stream, id, data, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)               { _server.LogError(ex); }
    }

    // ── Stop ─────────────────────────────────────────────────────────────────

    public Task StopAsync()
    {
        _cts?.Cancel();
        _sendChannel.Writer.TryComplete();
        _client.Dispose();
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await StopAsync().ConfigureAwait(false);
}
