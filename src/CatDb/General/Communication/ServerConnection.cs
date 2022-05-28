using System.Collections.Concurrent;
using System.Net.Sockets;

namespace CatDb.General.Communication
{
    public class ServerConnection
    {
        private Thread _receiver;
        private Thread _sender;
        private volatile bool _shutdown = false;

        public BlockingCollection<Packet> PendingPackets;

        public readonly TcpServer TcpServer;
        public readonly TcpClient TcpClient;

        public ServerConnection(TcpServer tcpServer, TcpClient tcpClient)
        {
            if (tcpServer == null)
                throw new ArgumentNullException("tcpServer == null");
            if (tcpClient == null)
                throw new ArgumentNullException("tcpClient == null");

            TcpServer = tcpServer;
            TcpClient = tcpClient;
        }

        public void Connect()
        {
            Disconnect();

            TcpServer.ServerConnections.TryAdd(this, this);
            PendingPackets = new BlockingCollection<Packet>();

            _shutdown = false;

            _receiver = new Thread(DoReceive);
            _receiver.Start();

            _sender = new Thread(DoSend);
            _sender.Start();
        }

        public void Disconnect()
        {
            if (!IsConnected)
                return;

            _shutdown = true;

            if (TcpClient != null)
                TcpClient.Close();

            var thread = _sender;
            if (thread is { ThreadState: ThreadState.Running })
            {
                if (!thread.Join(5000))
                    thread.Abort();
            }

            _sender = null;

            thread = _receiver;
            if (thread is { ThreadState: ThreadState.Running })
            {
                if (!thread.Join(5000))
                    thread.Abort();
            }

            _receiver = null;

            PendingPackets.Dispose();

            TcpServer.ServerConnections.TryRemove(this, out _);
        }

        public bool IsConnected => _receiver != null || _sender != null;

        private void DoReceive()
        {
            try
            {
                while (!TcpServer.ShutdownTokenSource.Token.IsCancellationRequested && !_shutdown && TcpClient.Connected)
                    ReceivePacket();
            }
            catch (Exception exc)
            {
                TcpServer.LogError(exc);
            }
            finally
            {
                Disconnect();
            }
        }

        private void ReceivePacket()
        {
            var reader = new BinaryReader(TcpClient.GetStream());

            var id = reader.ReadInt64();
            var size = reader.ReadInt32();
            TcpServer.BytesReceive += size;

            var packet = new Packet(new MemoryStream(reader.ReadBytes(size)))
            {
                Id = id
            };

            TcpServer.RecievedPackets.Add(new KeyValuePair<ServerConnection, Packet>(this, packet));
        }

        private void DoSend()
        {
            try
            {
                while (!TcpServer.ShutdownTokenSource.Token.IsCancellationRequested && !_shutdown && TcpClient.Connected)
                    SendPacket();
            }
            catch (OperationCanceledException exc)
            {
            }
            catch (Exception exc)
            {
                TcpServer.LogError(exc);
            }
            finally
            {
            }
        }

        private void SendPacket()
        {
            var token = TcpServer.ShutdownTokenSource.Token;
            var packet = PendingPackets.Take(token);
            TcpServer.BytesSent += packet.Response.Length;

            var writer = new BinaryWriter(TcpClient.GetStream());
            packet.Write(writer, packet.Response);
        }
    }
}