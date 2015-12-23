using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Vegvesen.Client;
using Vegvesen.Storage;

namespace Vegvesen.WebJob
{
    public class Functions
    {
        [NoAutomaticTrigger]
        public static async Task UpdateServiceDataAsync(
            TextWriter log, 
            string serviceConnectionString, 
            string storageConnectionString, 
            string serviceName, 
            string serviceUrl)
        {
            string logMessage;
            try
            {
                var storage = new VegvesenBlobStorage(storageConnectionString);
                var lastModified = await storage.GetLastModifiedTime(serviceName);

                var client = new VegvesenClient(serviceConnectionString);
                var result = await client.GetDataAsStreamAsync(serviceUrl, lastModified);

                if (result.Content != null)
                {
                    logMessage = string.Format("Saving updates for {0} since {1}...", serviceName, result.LastModified);
                    log.WriteLine(logMessage);

                    await storage.SaveEntryDataAsync(serviceName, result.Content, result.LastModified.Value);

                    logMessage = string.Format("Updates for {0} are saved.", serviceName);
                    log.WriteLine(logMessage);
                }
                else
                {
                    logMessage = string.Format("No updates for {0}.", serviceName);
                    log.WriteLine(logMessage);
                }
            }
            catch (Exception exception)
            {
                logMessage = string.Format("Error in {0}: {1}", serviceName, exception.Message);
                log.WriteLine(logMessage);
            }
        }
    }
}
