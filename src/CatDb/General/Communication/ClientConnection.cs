// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading.Channels;

namespace CatDb.General.Communication;

/// <summary>
/// High-performance async TCP client.
/// All socket I/O runs in two dedicated async loops (send / receive).
/// Callers enqueue a <see cref="Packet"/> via <see cref="SendAsync"/> and
/// await its <see cref="Packet.WaitAsync"/> — no thread ever blocks on I/O.
/// </summary>
public sealed class ClientConnection : IAsyncDisposable, IDisposable
{
    private long _idCounter;

    private TcpClient?                              _tcpClient;
    private Channel<Packet>?                        _sendChannel;
    private readonly ConcurrentDictionary<long, Packet> _pending = new();
    private CancellationTokenSource?                _cts;
    private Task?                                   _sendLoop;
    private Task?                                   _receiveLoop;

    // ── Sync mode state ─────────────────────────────────────────────────────
    // When the caller uses sync Start()/SendSync()/Stop(), we run blocking
    // socket I/O directly under _syncLock — no Task/async/Channel involved.
    private readonly CatDb.General.Threading.ReentrantLock _syncLock = new();
    private bool _syncMode;

    public string Host { get; }
    public int    Port { get; }

    public ClientConnection(string host = "localhost", int port = 7182)
    {
        Host = host;
        Port = port;
    }

    // ── Connect ──────────────────────────────────────────────────────────────

    public async Task StartAsync(CancellationToken ct = default)
    {
        if (_sendLoop is not null)
            throw new InvalidOperationException("ClientConnection is already started.");

        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        _sendChannel = Channel.CreateBounded<Packet>(new BoundedChannelOptions(256)
        {
            FullMode                      = BoundedChannelFullMode.Wait,
            SingleReader                  = true,
            AllowSynchronousContinuations = false,
        });

        _tcpClient = new TcpClient { NoDelay = true };
        await _tcpClient.ConnectAsync(Host, Port, _cts.Token).ConfigureAwait(false);

        var stream   = _tcpClient.GetStream();
        _sendLoop    = SendLoopAsync(stream, _cts.Token);
        _receiveLoop = ReceiveLoopAsync(stream, _cts.Token);
    }

    // ── Send ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Assigns an ID, enqueues the packet for sending, and returns a Task
    /// that completes when the matching response arrives from the server.
    /// </summary>
    public async Task<MemoryStream> SendAsync(Packet packet, CancellationToken ct = default)
    {
        if (_sendChannel is null)
            throw new InvalidOperationException("ClientConnection has not been started.");

        packet.Id = Interlocked.Increment(ref _idCounter);
        _pending[packet.Id] = packet;

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts!.Token);
        try
        {
            await _sendChannel.Writer.WriteAsync(packet, linked.Token).ConfigureAwait(false);
            return await packet.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch
        {
            _pending.TryRemove(packet.Id, out _);
            throw;
        }
    }

    // ── Loops ─────────────────────────────────────────────────────────────────

    private async Task SendLoopAsync(NetworkStream stream, CancellationToken ct)
    {
        try
        {
            await foreach (var packet in _sendChannel!.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                await FrameProtocol.WriteAsync(stream, packet.Id, packet.Request, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { FailAll(ex); }
    }

    private async Task ReceiveLoopAsync(NetworkStream stream, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var (id, ms) = await FrameProtocol.ReadAsync(stream, ct).ConfigureAwait(false);
                if (_pending.TryRemove(id, out var packet))
                    packet.SetResponse(ms);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { FailAll(ex); }
    }

    private void FailAll(Exception ex)
    {
        foreach (var packet in _pending.Values)
            packet.SetException(ex);
        _pending.Clear();
    }

    // ── Stop ─────────────────────────────────────────────────────────────────

    public async Task StopAsync()
    {
        _sendChannel?.Writer.TryComplete();
        _cts?.Cancel();
        if (_sendLoop    is not null) await _sendLoop.ConfigureAwait(false);
        if (_receiveLoop is not null) await _receiveLoop.ConfigureAwait(false);
        _tcpClient?.Dispose();
        _sendLoop    = null;
        _receiveLoop = null;
    }

    public async ValueTask DisposeAsync() => await StopAsync().ConfigureAwait(false);

    // ── True sync transport (NO Task / NO await / NO GetResult) ──────────────
    // Used by callers that need a synchronous API (e.g. IStorageEngine sync
    // Execute path). Cannot coexist with async transport on the same instance.

    public void Start()
    {
        if (_sendLoop is not null || _syncMode)
            throw new InvalidOperationException("ClientConnection is already started.");

        _tcpClient = new TcpClient { NoDelay = true };
        _tcpClient.Connect(Host, Port);
        _syncMode = true;
    }

    public MemoryStream SendSync(Packet packet)
    {
        if (!_syncMode || _tcpClient is null)
            throw new InvalidOperationException("ClientConnection is not in sync mode. Use Start()+SendSync, or StartAsync+SendAsync.");

        using (_syncLock.Lock())
        {
            packet.Id = Interlocked.Increment(ref _idCounter);
            var stream = _tcpClient.GetStream();
            FrameProtocol.WriteSync(stream, packet.Id, packet.Request);
            var (_, response) = FrameProtocol.ReadSync(stream);
            return response;
        }
    }

    public void Stop()
    {
        if (!_syncMode) return;
        using (_syncLock.Lock())
        {
            _tcpClient?.Close();
            _tcpClient = null;
            _syncMode = false;
        }
    }

    public void Dispose() => Stop();
}
