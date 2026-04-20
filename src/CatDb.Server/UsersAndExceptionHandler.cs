using System.Collections.Concurrent;
using System.Net;
using System.Windows.Forms;
using CatDb.General.Communication;

namespace CatDb.Server;

public class UsersAndExceptionHandler
{
    private Thread _worker;
    private volatile bool _shutDown;
    private bool _disconnecting;
    private volatile bool _hasNewExceptions;
    private volatile bool _refreshed;

    private readonly ConcurrentDictionary<ServerConnection, string> _clientsList = new();
    private readonly List<KeyValuePair<string, string>> _exceptionsList = new(100);
    private readonly ConcurrentBag<string> _clientsForDisconnects = new();

    public bool IsFinishRefresh   => _refreshed;
    public bool IsWorking         => _worker is not null;
    public bool IsDisconnecting   => _disconnecting;

    public void Start()
    {
        Stop();
        _shutDown = false;
        _worker = new Thread(RefreshList);
        _worker.Start();
    }

    public void Stop()
    {
        if (!IsWorking) return;

        _shutDown = true;
        _worker?.Join(5_000);
        _worker = null;
    }

    public void Disconnect(ListView.SelectedListViewItemCollection clientsForDisconnect)
    {
        if (!IsWorking) return;

        foreach (ListViewItem client in clientsForDisconnect)
            _clientsForDisconnects.Add(client.Text);

        _disconnecting = true;
    }

    private void DisconnectClient(string client)
    {
        lock (_clientsList)
        {
            foreach (var item in _clientsList)
            {
                if (item.Value.Equals(client))
                    item.Key.Disconnect();
            }
        }
    }

    private void RefreshList()
    {
        while (!_shutDown)
        {
            lock (_clientsList)
                _clientsList.Clear();

            var id = 0;
            foreach (var connection in Program.StorageEngineServer.TcpServer.ServerConnections)
            {
                if (connection.Key.TcpClient.Connected)
                {
                    var clientIp = IPAddress.Parse(
                        ((IPEndPoint)connection.Key.TcpClient.Client.RemoteEndPoint).Address.ToString()).ToString();
                    _clientsList.TryAdd(connection.Key, $"{clientIp} ID:{id++}");
                }
            }

            _refreshed = true;

            if (Program.StorageEngineServer.TcpServer.Errors.TryDequeue(out var error))
            {
                lock (_exceptionsList)
                {
                    _exceptionsList.Insert(0, new KeyValuePair<string, string>(error.Key.ToString(), error.Value.Message));
                    _hasNewExceptions = true;
                }
            }

            if (IsDisconnecting)
            {
                foreach (var client in _clientsForDisconnects)
                    DisconnectClient(client);
                _disconnecting = false;
            }

            Thread.Sleep(300);
        }

        _worker = null;
    }

    public IEnumerable<string> GetClients()
    {
        _refreshed = false;
        foreach (var client in _clientsList)
            yield return client.Value;
    }

    public IEnumerable<KeyValuePair<string, string>> GetExceptions()
    {
        if (!_hasNewExceptions) yield break;

        lock (_exceptionsList)
        {
            foreach (var exception in _exceptionsList)
                yield return exception;

            _exceptionsList.Clear();
            _hasNewExceptions = false;
        }
    }
}
