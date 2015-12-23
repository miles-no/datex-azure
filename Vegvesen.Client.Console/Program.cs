using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vegvesen.Storage;

namespace Vegvesen.Client.Console
{
    class Program
    {
        private const string DataDir = @"H:\Documents\Projects\Git\MilesDatex\Data";
        private static string ConnectionString;

        static void Main(string[] args)
        {
            ConnectionString = ConfigurationManager.ConnectionStrings["VegvesenDatex"].ConnectionString;

            foreach (var serviceUrl in VegvesenClient.ServiceUrls.Values)
            {
                RequestAsync(serviceUrl);

                Task.Delay(60000 / VegvesenClient.ServiceUrls.Count).Wait();
            }

            System.Console.ReadKey();
        }

        static async void RequestAsync(string serviceUrl)
        {
            while (true)
            {
                try
                {
                    var storage = new VegvesenFileStorage(DataDir);
                    var lastModified = await storage.GetLastModifiedTime(serviceUrl);

                    var client = new VegvesenClient(ConnectionString);
                    var result = await client.GetDataAsStreamAsync(serviceUrl, lastModified);

                    if (result.Content != null)
                    {
                        System.Console.WriteLine("Saving updates for {0} since {1}...", serviceUrl, result.LastModified);
                        await storage.SaveEntryDataAsync(serviceUrl, result.Content, result.LastModified.Value);
                        System.Console.WriteLine("Updates for {0} are saved.", serviceUrl);
                    }
                    else
                    {
                        System.Console.WriteLine("No updates for {0}.", serviceUrl);
                    }
                }
                catch (Exception exception)
                {
                    System.Console.WriteLine("Error in {0}: {1}", serviceUrl, exception.Message);
                }

                await Task.Delay(60000);
            }
        }
    }
}
