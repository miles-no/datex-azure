using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Vegvesen.Client;

namespace Vegvesen.WebJob
{
    class Program
    {
        static void Main()
        {
            var vegvesenConnectionString = ConfigurationManager.ConnectionStrings["VegvesenDatex"].ConnectionString;
            var blobStorageConnectionString = ConfigurationManager.ConnectionStrings["AzureWebJobsStorage"].ConnectionString;

            var host = new JobHost();

            var tasks = new List<Task>();
            foreach (var serviceUrl in VegvesenClient.ServiceUrls)
            {
                tasks.Add(host.CallAsync(typeof(Functions).GetMethod("UpdateServiceDataAsync"),
                    new
                    {
                        serviceConnectionString = vegvesenConnectionString,
                        storageConnectionString = blobStorageConnectionString,
                        serviceUrl = serviceUrl
                    }));
            }
            Task.WaitAll(tasks.ToArray());
        }
    }
}
