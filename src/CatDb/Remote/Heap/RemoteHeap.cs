using CatDb.General.Communication;
using CatDb.WaterfallTree;

namespace CatDb.Remote.Heap;

public class RemoteHeap : IHeap
{
    public ClientConnection Client { get; private set; }

    public RemoteHeap(string host, int port)
    {
        Client = new ClientConnection(host, port);
        // Safe sync connect: Task.Run strips SynchronizationContext
        Task.Run(() => Client.StartAsync(CancellationToken.None)).GetAwaiter().GetResult();
    }

    // Helper: fire a request and return the response stream (sync bridge)
    private MemoryStream Execute(MemoryStream request)
    {
        var packet = new Packet(request);
        return Task.Run(() => Client.SendAsync(packet, CancellationToken.None))
                   .GetAwaiter().GetResult();
    }

    #region IHeap members

    public long ObtainNewHandle()
    {
        var ms = new MemoryStream();
        ObtainHandleCommand.WriteRequest(new BinaryWriter(ms));
        var response = Execute(ms);
        return ObtainHandleCommand.ReadResponse(new BinaryReader(response)).Handle;
    }

    public void Release(long handle)
    {
        var ms = new MemoryStream();
        ReleaseHandleCommand.WriteRequest(new BinaryWriter(ms), handle);
        Execute(ms);
    }

    public bool Exists(long handle)
    {
        var ms = new MemoryStream();
        HandleExistCommand.WriteRequest(new BinaryWriter(ms), handle);
        var response = Execute(ms);
        return HandleExistCommand.ReadResponse(new BinaryReader(response)).Exist;
    }

    public void Write(long handle, byte[] buffer, int index, int count)
    {
        var ms = new MemoryStream();
        WriteCommand.WriteRequest(new BinaryWriter(ms), handle, index, count, buffer);
        Execute(ms);
    }

    public byte[] Read(long handle)
    {
        var ms = new MemoryStream();
        ReadCommand.WriteRequest(new BinaryWriter(ms), handle);
        var response = Execute(ms);
        return ReadCommand.ReadResponse(new BinaryReader(response)).Buffer;
    }

    public void Commit()
    {
        var ms = new MemoryStream();
        CommitCommand.WriteRequest(new BinaryWriter(ms));
        Execute(ms);
    }

    public void Close()
    {
        var ms = new MemoryStream();
        CloseCommand.WriteRequest(new BinaryWriter(ms));
        Execute(ms);
    }

    public byte[] Tag
    {
        get
        {
            var ms = new MemoryStream();
            GetTagCommand.WriteRequest(new BinaryWriter(ms));
            var response = Execute(ms);
            return GetTagCommand.ReadResponse(new BinaryReader(response)).Tag;
        }
        set
        {
            var ms = new MemoryStream();
            SetTagCommand.WriteRequest(new BinaryWriter(ms), value);
            Execute(ms);
        }
    }

    public long DataSize
    {
        get
        {
            var ms = new MemoryStream();
            DataBaseSizeCommand.WriteRequest(new BinaryWriter(ms));
            var response = Execute(ms);
            return DataBaseSizeCommand.ReadResponse(new BinaryReader(response)).DataBaseSize;
        }
    }

    public long Size
    {
        get
        {
            var ms = new MemoryStream();
            SizeCommand.WriteRequest(new BinaryWriter(ms));
            var response = Execute(ms);
            return SizeCommand.ReadResponse(new BinaryReader(response)).DataBaseSize;
        }
    }

    #endregion
}
