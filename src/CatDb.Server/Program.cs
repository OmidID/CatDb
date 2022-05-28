using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using CatDb.Database;
using CatDb.Data;
using CatDb.Database.Operations;
using CatDb.WaterfallTree;
using System.IO;
using System.Configuration;
using CatDb.Remote;

namespace CatDb.Server
{
    static class Program
    {
        internal static StorageEngineServer StorageEngineServer;
        /// <summary>
        /// The main entry point for the application.
        /// </summary>

        static void Main()
        {
            string serviceMode = ConfigurationSettings.AppSettings["ServiceMode"];
            bool isService = bool.Parse(serviceMode);

            if (!isService)
                new CatDbService().Start();
            else
            {
                ServiceBase[] ServicesToRun;
                ServicesToRun = new ServiceBase[] 
                { 
                    new CatDbService() 
                };
                ServiceBase.Run(ServicesToRun);
            }
        }
    }
}
