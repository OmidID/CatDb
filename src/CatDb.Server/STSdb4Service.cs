using System.Configuration;
using System.ServiceProcess;
using System.Windows.Forms;
using CatDb.Database;
using CatDb.General.Communication;
using CatDb.Remote;

namespace CatDb.Server;

public partial class CatDbService : ServiceBase
{
    internal static CatDbService Service { get; private set; }

    private MainForm _form;
    private IStorageEngine _storageEngine;
    private TcpServer _tcpServer;

    public CatDbService() => InitializeComponent();

    public void Start() => OnStart([]);

    protected override void OnStart(string[] args)
    {
        var fileName = ConfigurationManager.AppSettings["FileName"];
        var port     = int.Parse(ConfigurationManager.AppSettings["Port"]);
        Service = this;

        _storageEngine = Database.CatDb.FromFile(fileName);
        _tcpServer     = new TcpServer(port);

        Program.StorageEngineServer = new StorageEngineServer(_storageEngine, _tcpServer);
        Program.StorageEngineServer.Start();

        _form = new MainForm();
        Application.Run(_form);

        Program.StorageEngineServer.Stop();
        _storageEngine.Close();
    }

    protected override void OnStop()
    {
        _form?.Close();
        _storageEngine?.Close();
        Program.StorageEngineServer?.Stop();
    }
}
