using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace CatDb.General.Communication
{
    public class TcpServer
    {
        private Thread _worker;

        public BlockingCollection<KeyValuePair<ServerConnection, Packet>> RecievedPackets;
        public CancellationTokenSource ShutdownTokenSource { get; private set; }

        public readonly ConcurrentQueue<KeyValuePair<DateTime, Exception>> Errors = new();
        public readonly ConcurrentDictionary<ServerConnection, ServerConnection> ServerConnections = new();

        public int Port { get; private set; }

        public long BytesReceive { get; internal set; }
        public long BytesSent { get; internal set; }

        public TcpServer(int port = 7182)
        {
            Port = port;
        }

        public void Start(int boundedCapacity = 64)
        {
            Stop();

            RecievedPackets = new BlockingCollection<KeyValuePair<ServerConnection, Packet>>(boundedCapacity);
            ServerConnections.Clear();

            ShutdownTokenSource = new CancellationTokenSource();

            _worker = new Thread(DoWork);
            _worker.Start();
        }

        public void Stop()
        {
            if (!IsWorking)
                return;

            if (ShutdownTokenSource != null)
                ShutdownTokenSource.Cancel(false);

            DisconnectConnections();

            var thread = _worker;
            if (thread != null)
            {
                if (!thread.Join(5000))
                    thread.Abort();
            }
        }

        public bool IsWorking => _worker != null;

        private void DoWork()
        {
            TcpListener listener = null;
            try
            {
                listener = new TcpListener(IPAddress.Any, Port);
                listener.Start();

                while (!ShutdownTokenSource.Token.IsCancellationRequested)
                {
                    if (listener.Pending())
                    {
                        try
                        {
                            var client = listener.AcceptTcpClient();
                            var serverConnection = new ServerConnection(this, client);
                            serverConnection.Connect();
                        }
                        catch (Exception exc)
                        {
                            LogError(exc);
                        }
                    }

                    Thread.Sleep(10);
                }
            }
            catch (Exception exc)
            {
                LogError(exc);
            }
            finally
            {
                if (listener != null)
                    listener.Stop();

                _worker = null;
            }
        }

        public void LogError(Exception exc)
        {
            while (Errors.Count > 100)
            {
                Errors.TryDequeue(out _);
            }

            Errors.Enqueue(new KeyValuePair<DateTime, Exception>(DateTime.Now, exc));
        }

        private void DisconnectConnections()
        {
            foreach (var connection in ServerConnections)
                connection.Key.Disconnect();
        }
    }
}
