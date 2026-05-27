#pragma warning disable CS8602, CS8604, CS8625, CS8600, CS8603, CS8601, CS8618, CS8622, CS8629
using CatDb.General.Communication;
using CatDb.WaterfallTree;

namespace CatDb.Remote.Heap;

public sealed class HeapServer
{
    private readonly IHeap    _heap;
    private readonly TcpServer _tcpServer;

    public HeapServer(IHeap heap, TcpServer tcpServer)
    {
        _heap      = heap      ?? throw new ArgumentNullException(nameof(heap));
        _tcpServer = tcpServer ?? throw new ArgumentNullException(nameof(tcpServer));

        _tcpServer.PacketReceived = HandlePacketAsync;
    }

    public HeapServer(IHeap heap, int port = 7183)
        : this(heap, new TcpServer(port))
    {
    }

    // ── Start / Stop ─────────────────────────────────────────────────────────

    public Task StartAsync(CancellationToken ct = default) => _tcpServer.StartAsync(ct);
    public Task StopAsync()                                => _tcpServer.StopAsync();

    public bool IsRunning    => _tcpServer.IsRunning;
    public int  ClientsCount => _tcpServer.ServerConnections.Count;

    // ── Packet handler ───────────────────────────────────────────────────────

    private async Task HandlePacketAsync(ServerConnection conn, Packet packet, CancellationToken ct)
    {
        try
        {
            var responseMs = await Task.Run(() => ProcessPacket(packet.Request), ct).ConfigureAwait(false);
            await conn.SendResponseAsync(packet.Id, responseMs, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _tcpServer.LogError(ex);
        }
    }

    private MemoryStream ProcessPacket(MemoryStream requestStream)
    {
        var reader = new BinaryReader(requestStream);
        var ms     = new MemoryStream();
        var writer = new BinaryWriter(ms);

        var code = (RemoteHeapCommandCodes)reader.ReadByte();

        switch (code)
        {
            case RemoteHeapCommandCodes.ObtainHandle:
                ObtainHandleCommand.WriteResponse(writer, _heap.ObtainNewHandle());
                break;

            case RemoteHeapCommandCodes.ReleaseHandle:
                _heap.Release(ReleaseHandleCommand.ReadRequest(reader).Handle);
                break;

            case RemoteHeapCommandCodes.HandleExist:
                HandleExistCommand.WriteResponse(writer, _heap.Exists(HandleExistCommand.ReadRequest(reader).Handle));
                break;

            case RemoteHeapCommandCodes.WriteCommand:
                var wcmd = WriteCommand.ReadRequest(reader);
                _heap.Write(wcmd.Handle, wcmd.Buffer, wcmd.Index, wcmd.Count);
                break;

            case RemoteHeapCommandCodes.ReadCommand:
                ReadCommand.WriteResponse(writer, _heap.Read(ReadCommand.ReadRequest(reader).Handle));
                break;

            case RemoteHeapCommandCodes.CommitCommand:
                _heap.Commit();
                break;

            case RemoteHeapCommandCodes.CloseCommand:
                _heap.Close();
                break;

            case RemoteHeapCommandCodes.SetTag:
                _heap.Tag = SetTagCommand.ReadRequest(reader).Tag;
                break;

            case RemoteHeapCommandCodes.GetTag:
                GetTagCommand.WriteResponse(writer, _heap.Tag);
                break;

            case RemoteHeapCommandCodes.Size:
                SizeCommand.WriteResponse(writer, _heap.Size);
                break;

            case RemoteHeapCommandCodes.DataBaseSize:
                DataBaseSizeCommand.WriteResponse(writer, _heap.DataSize);
                break;
        }

        ms.Position = 0;
        return ms;
    }
}
