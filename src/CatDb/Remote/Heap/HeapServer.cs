using CatDb.General.Communication;
using CatDb.WaterfallTree;

namespace CatDb.Remote.Heap
{
    public class HeapServer
    {
        private CancellationTokenSource _shutdownTokenSource;
        private Thread _worker;

        private readonly IHeap _heap;
        private readonly TcpServer _tcpServer;

        public HeapServer(IHeap heap, TcpServer tcpServer)
        {
            if (heap == null)
                throw new ArgumentNullException("heap");
            if (tcpServer == null)
                throw new ArgumentNullException("tcpServer");

            _heap = heap;
            _tcpServer = tcpServer;
        }

        public HeapServer(IHeap heap, int port = 7183)
            : this(heap, new TcpServer(port))
        {
        }

        public void Start()
        {
            Stop();

            _shutdownTokenSource = new CancellationTokenSource();

            _worker = new Thread(DoWork);
            _worker.Start();
        }

        public void Stop()
        {
            if (!IsWorking)
                return;

            _shutdownTokenSource.Cancel(false);

            var thread = _worker;
            if (thread != null)
            {
                if (!thread.Join(5000))
                    thread.Abort();
            }
            _heap.Close();
        }

        private void DoWork()
        {
            try
            {
                _tcpServer.Start();

                while (!_shutdownTokenSource.Token.IsCancellationRequested)
                {
                    try
                    {
                        var order = _tcpServer.RecievedPackets.Take(_shutdownTokenSource.Token);

                        var reader = new BinaryReader(order.Value.Request);
                        var ms = new MemoryStream();
                        var writer = new BinaryWriter(ms);

                        var code = (RemoteHeapCommandCodes)reader.ReadByte();

                        switch (code)
                        {
                            case RemoteHeapCommandCodes.ObtainHandle:
                                ObtainHandleCommand.WriteResponse(writer, _heap.ObtainNewHandle());
                                break;

                            case RemoteHeapCommandCodes.ReleaseHandle:
                                {
                                    var handle = ReleaseHandleCommand.ReadRequest(reader).Handle;
                                    _heap.Release(handle);
                                    break;
                                }

                            case RemoteHeapCommandCodes.HandleExist:
                                {
                                    var handle = HandleExistCommand.ReadRequest(reader).Handle;
                                    HandleExistCommand.WriteResponse(writer, _heap.Exists(handle));
                                    break;
                                }

                            case RemoteHeapCommandCodes.WriteCommand:
                                var cmd = WriteCommand.ReadRequest(reader);
                                _heap.Write(cmd.Handle, cmd.Buffer, cmd.Index, cmd.Count);
                                break;

                            case RemoteHeapCommandCodes.ReadCommand:
                                {
                                    var handle = ReadCommand.ReadRequest(reader).Handle;
                                    ReadCommand.WriteResponse(writer, _heap.Read(handle));
                                    break;
                                }

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

                            default:
                                break;
                        }

                        ms.Position = 0;
                        order.Value.Response = ms;
                        order.Key.PendingPackets.Add(order.Value);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception exc)
                    {
                        _tcpServer.LogError(exc);
                    }
                }
            }
            catch (Exception exc)
            {
                _tcpServer.LogError(exc);
            }
            finally
            {
                _tcpServer.Stop();
                _worker = null;
            }
        }

        public bool IsWorking => _worker != null;

        public int ClientsCount => _tcpServer.ServerConnections.Count;
    }
}