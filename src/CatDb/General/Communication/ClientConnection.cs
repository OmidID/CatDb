using System.Collections.Concurrent;
using System.Net.Sockets;

namespace CatDb.General.Communication
{
    public class ClientConnection
    {
        private long _id = 0;

        public TcpClient TcpClient { get; private set; }

        public BlockingCollection<Packet> PendingPackets;
        public ConcurrentDictionary<long, Packet> SentPackets;

        private CancellationTokenSource _shutdownTokenSource;

        private Thread _sendWorker;
        private Thread _recieveWorker;

        public readonly string MachineName;
        public readonly int Port;

        public ClientConnection(string machineName = "localhost", int port = 7182)
        {
            MachineName = machineName;
            Port = port;
        }

        public void Send(Packet packet)
        {
            if (!IsWorking)
                throw new Exception("Client connection is not started.");

            packet.Id = Interlocked.Increment(ref _id);
            PendingPackets.Add(packet, _shutdownTokenSource.Token);
        }

        public void Start(int boundedCapacity = 64, int recieveTimeout = 0, int sendTimeout = 0)
        {
            if (IsWorking)
                throw new Exception("Client connection is already started.");

            PendingPackets = new BlockingCollection<Packet>(boundedCapacity);
            SentPackets = new ConcurrentDictionary<long, Packet>();
            _shutdownTokenSource = new CancellationTokenSource();

            TcpClient = new TcpClient();
            TcpClient.ReceiveTimeout = recieveTimeout;
            TcpClient.SendTimeout = sendTimeout;
            TcpClient.Connect(MachineName, Port);
            var networkStream = TcpClient.GetStream();

            _sendWorker = new Thread(new ParameterizedThreadStart(DoSend));
            _recieveWorker = new Thread(new ParameterizedThreadStart(DoRecieve));

            _sendWorker.Start(networkStream);
            _recieveWorker.Start(networkStream);
        }

        public void Stop()
        {
            if (!IsWorking)
                return;

            _shutdownTokenSource.Cancel(false);

            var thread = _recieveWorker;
            if (thread != null)
            {
                if (thread.Join(2000))
                    thread.Abort();
            }

            thread = _sendWorker;
            if (thread != null)
            {
                if (thread.Join(2000))
                    thread.Abort();
            }

            PendingPackets = null;
            SetException(new Exception("Client stopped"));
            _shutdownTokenSource = null;
        }

        public bool IsWorking => _sendWorker != null || _recieveWorker != null;

        private void DoSend(object state)
        {
            var writer = new BinaryWriter((NetworkStream)state);

            try
            {
                while (!Shutdown.IsCancellationRequested)
                {
                    var packet = PendingPackets.Take(Shutdown);

                    SentPackets.TryAdd(packet.Id, packet);
                    packet.Write(writer, packet.Request);
                    writer.Flush();
                }
            }
            catch (Exception e)
            {
                SetException(e);
            }
            finally
            {
                _sendWorker = null;
            }
        }

        private void DoRecieve(object state)
        {
            var reader = new BinaryReader((NetworkStream)state);

            try
            {
                while (!Shutdown.IsCancellationRequested)
                {
                    var id = reader.ReadInt64();
                    var size = reader.ReadInt32();
                    var response = new MemoryStream(reader.ReadBytes(size));

                    Packet packet = null;
                    if (SentPackets.TryRemove(id, out packet))
                    {
                        packet.Response = response;
                        packet.ResultEvent.Set();
                    }
                }
            }
            catch (Exception e)
            {
                SetException(e);
            }
            finally
            {
                _recieveWorker = null;
            }
        }

        private void SetException(Exception exception)
        {
            lock (SentPackets)
            {
                foreach (var packet in SentPackets.Values)
                {
                    packet.Exception = exception;
                    packet.ResultEvent.Set();
                }

                SentPackets.Clear();
            }
        }

        private CancellationToken Shutdown => _shutdownTokenSource.Token;

        public int BoundedCapacity
        {
            get
            {
                if (!IsWorking)
                    throw new Exception("Client connection is not started.");

                return PendingPackets.BoundedCapacity;
            }
        }
    }
}