using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.WindowsAzure.Storage.Blob;
using Vegvesen.Client;
using Vegvesen.Storage;

namespace Vegvesen.WebJob
{
    public class Functions
    {
        [NoAutomaticTrigger]
        public static async Task UpdateServiceDataAsync(TextWriter log, string serviceConnectionString, string storageConnectionString, string serviceUrl)
        {
            try
            {
                var storage = new VegvesenBlobStorage(storageConnectionString);
                var lastModified = await storage.GetLastModifiedTime(serviceUrl);

                var client = new VegvesenClient(serviceConnectionString);
                var result = await client.GetDataAsStreamAsync(serviceUrl, lastModified);

                if (result.Content != null)
                {
                    log.WriteLine("Saving updates for {0} since {1}...", serviceUrl, result.LastModified);
                    await storage.SaveEntryDataAsync(serviceUrl, result.Content, result.LastModified.Value);
                    log.WriteLine("Updates for {0} are saved.", serviceUrl);
                }
                else
                {
                    log.WriteLine("No updates for {0}.", serviceUrl);
                }
            }
            catch (Exception exception)
            {
                log.WriteLine("Error in {0}: {1}", serviceUrl, exception.Message);
            }
        }
    }
}
