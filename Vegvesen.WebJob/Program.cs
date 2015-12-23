using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Vegvesen.Client;

namespace Vegvesen.WebJob
{
    class Program
    {
        private const bool LocalExection = false;

        static void Main()
        {
            var vegvesenConnectionString = ConfigurationManager.ConnectionStrings["VegvesenDatex"].ConnectionString;
            var blobStorageConnectionString = ConfigurationManager.ConnectionStrings["AzureWebJobsStorage"].ConnectionString;

            var host = new JobHost();

            var tasks = new List<Task>();
            foreach (var service in VegvesenClient.ServiceUrls)
            {
                if (LocalExection)
                {
                    var textWriter = new StreamWriter(".\\Test.txt");
                    Functions.UpdateServiceDataAsync(
                        textWriter,
                        vegvesenConnectionString,
                        blobStorageConnectionString,
                        service.Key,
                        service.Value).Wait();
                }
                else
                {
                    tasks.Add(host.CallAsync(typeof(Functions).GetMethod("UpdateServiceDataAsync"),
                        new
                        {
                            serviceConnectionString = vegvesenConnectionString,
                            storageConnectionString = blobStorageConnectionString,
                            serviceName = service.Key,
                            serviceUrl = service.Value,
                        }));
                }
            }

            if (tasks.Any())
                Task.WaitAll(tasks.ToArray());
        }
    }
}
