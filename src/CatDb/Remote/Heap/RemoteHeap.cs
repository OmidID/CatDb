using CatDb.General.Communication;
using CatDb.WaterfallTree;

namespace CatDb.Remote.Heap
{
    public class RemoteHeap : IHeap
    {
        public ClientConnection Client { get; private set; }

        public RemoteHeap(string host, int port)
        {
            Client = new ClientConnection(host, port);
            Client.Start();
        }

        #region IHeap members

        public long ObtainNewHandle()
        {
            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms);
            ObtainHandleCommand.WriteRequest(writer);

            var packet = new Packet(ms);
            Client.Send(packet);
            packet.Wait();

            return ObtainHandleCommand.ReadResponse(new BinaryReader(packet.Response)).Handle;
        }

        public void Release(long handle)
        {
            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms);
            ReleaseHandleCommand.WriteRequest(writer, handle);

            var packet = new Packet(ms);
            Client.Send(packet);
        }

        public bool Exists(long handle)
        {
            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms);
            HandleExistCommand.WriteRequest(writer, handle);

            var packet = new Packet(ms);
            Client.Send(packet);
            packet.Wait();

            return HandleExistCommand.ReadResponse(new BinaryReader(packet.Response)).Exist;
        }

        public void Write(long handle, byte[] buffer, int index, int count)
        {
            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms);
            WriteCommand.WriteRequest(writer, handle, index, count, buffer);

            var packet = new Packet(ms);
            Client.Send(packet);
        }

        public byte[] Read(long handle)
        {
            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms);
            ReadCommand.WriteRequest(writer, handle);

            var packet = new Packet(ms);
            Client.Send(packet);
            packet.Wait();

            return ReadCommand.ReadResponse(new BinaryReader(packet.Response)).Buffer;
        }

        public void Commit()
        {
            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms);
            CommitCommand.WriteRequest(writer);

            var packet = new Packet(ms);
            Client.Send(packet);
        }

        public void Close()
        {
            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms);
            CloseCommand.WriteRequest(writer);

            var packet = new Packet(ms);
            Client.Send(packet);
        }

        public byte[] Tag
        {
            get
            {
                var ms = new MemoryStream();
                var writer = new BinaryWriter(ms);
                GetTagCommand.WriteRequest(writer);

                var packet = new Packet(ms);
                Client.Send(packet);
                packet.Wait();

                return GetTagCommand.ReadResponse(new BinaryReader(packet.Response)).Tag;
            }
            set
            {
                var ms = new MemoryStream();
                var writer = new BinaryWriter(ms);
                SetTagCommand.WriteRequest(writer, value);

                var packet = new Packet(ms);
                Client.Send(packet);
            }
        }

        public long DataSize
        {
            get
            {
                var ms = new MemoryStream();
                var writer = new BinaryWriter(ms);
                DataBaseSizeCommand.WriteRequest(writer);

                var packet = new Packet(ms);
                Client.Send(packet);
                packet.Wait();

                return DataBaseSizeCommand.ReadResponse(new BinaryReader(packet.Response)).DataBaseSize;
            }
        }

        public long Size
        {
            get
            {
                var ms = new MemoryStream();
                var writer = new BinaryWriter(ms);
                SizeCommand.WriteRequest(writer);

                var packet = new Packet(ms);
                Client.Send(packet);
                packet.Wait();

                return SizeCommand.ReadResponse(new BinaryReader(packet.Response)).DataBaseSize;
            }
        }

        #endregion
    }
}