using System.Collections.Concurrent;
using System.Net.Sockets;

namespace CatDb.General.Communication
{
    public class ServerConnection
    {
        private Thread Receiver;
        private Thread Sender;
        private volatile bool Shutdown = false;

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

            Shutdown = false;

            Receiver = new Thread(DoReceive);
            Receiver.Start();

            Sender = new Thread(DoSend);
            Sender.Start();
        }

        public void Disconnect()
        {
            if (!IsConnected)
                return;

            Shutdown = true;

            if (TcpClient != null)
                TcpClient.Close();

            var thread = Sender;
            if (thread != null && thread.ThreadState == ThreadState.Running)
            {
                if (!thread.Join(5000))
                    thread.Abort();
            }

            Sender = null;

            thread = Receiver;
            if (thread != null && thread.ThreadState == ThreadState.Running)
            {
                if (!thread.Join(5000))
                    thread.Abort();
            }

            Receiver = null;

            PendingPackets.Dispose();

            ServerConnection reference;
            TcpServer.ServerConnections.TryRemove(this, out reference);
        }

        public bool IsConnected => Receiver != null || Sender != null;

        private void DoReceive()
        {
            try
            {
                while (!TcpServer.ShutdownTokenSource.Token.IsCancellationRequested && !Shutdown && TcpClient.Connected)
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

            var packet = new Packet(new MemoryStream(reader.ReadBytes(size)));
            packet.ID = id;

            TcpServer.RecievedPackets.Add(new KeyValuePair<ServerConnection, Packet>(this, packet));
        }

        private void DoSend()
        {
            try
            {
                while (!TcpServer.ShutdownTokenSource.Token.IsCancellationRequested && !Shutdown && TcpClient.Connected)
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