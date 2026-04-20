using System.Configuration;
using System.ServiceProcess;
using CatDb.Remote;

namespace CatDb.Server;

static class Program
{
    internal static StorageEngineServer StorageEngineServer;

    static void Main()
    {
        var isService = bool.Parse(ConfigurationManager.AppSettings["ServiceMode"]);

        if (!isService)
            new CatDbService().Start();
        else
            ServiceBase.Run([new CatDbService()]);
    }
}
