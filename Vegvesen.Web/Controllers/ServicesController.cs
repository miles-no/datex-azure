using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Vegvesen.Client;
using Vegvesen.Storage;
using Vegvesen.Web.Models;

namespace Vegvesen.Web.Controllers
{
    public class ServicesController : ApiController
    {
        private const string DataDir = @"H:\Documents\Projects\Git\MilesDatex\Data";

        // GET api/services
        public IEnumerable<string> Get()
        {
            return VegvesenClient.ServiceUrls;
        }

        // GET api/services/{name}
        public async Task<IHttpActionResult> Get([FromUri] string service)
        {
            var serviceUrl = GetServiceUrl(service);
            
            if (serviceUrl != null)
            {
                return Ok(await GetServiceDataAsync(serviceUrl));
            }

            return StatusCode(HttpStatusCode.NotFound);
        }

        // GET api/services/{name}
        public async Task<IHttpActionResult> Get([FromUri] string service, [FromUri] string entry)
        {
            var serviceUrl = GetServiceUrl(service);

            if (serviceUrl != null)
            {
                return Ok(await GetEntryDataAsync(serviceUrl, entry));
            }

            return StatusCode(HttpStatusCode.NotFound);
        }

        public void Post([FromUri] string service)
        {
            var serviceUrl = GetServiceUrl(service);

            if (serviceUrl != null)
            {
                Task.Factory.StartNew(() => UpdateServiceDataAsync(serviceUrl));
            }
        }

        private string GetServiceUrl(string serviceName)
        {
            return VegvesenClient.ServiceUrls.SingleOrDefault(
                x => string.Equals(x, serviceName, StringComparison.InvariantCultureIgnoreCase));
        }

        private async Task<ServiceData> GetServiceDataAsync(string serviceName)
        {
            var storage = new VegvesenFileStorage(DataDir);

            var serviceData = new ServiceData();
            serviceData.ServiceName = serviceName;
            serviceData.LastModified = await storage.GetLastModifiedTime(serviceName);
            serviceData.DataCollection = await storage.GetEntryNamesAsync(serviceName);

            return serviceData;
        }

        private async Task<string> GetEntryDataAsync(string serviceName, string entryName)
        {
            var storage = new VegvesenFileStorage(DataDir);

            var entryData = await storage.GetEntryDataAsync(serviceName, entryName);

            return entryData;
        }

        private async Task UpdateServiceDataAsync(string serviceUrl)
        {
            var storage = new VegvesenFileStorage(DataDir);
            var lastModified = await storage.GetLastModifiedTime(serviceUrl);

            var connectionString = ConfigurationManager.ConnectionStrings["VegvesenDatex"].ConnectionString;
            var client = new VegvesenClient(connectionString);

            var result = await client.GetDataAsStreamAsync(serviceUrl, lastModified);
            if (result.Content != null)
            {
                storage.SaveEntryDataAsync(serviceUrl, result.Content, result.LastModified.Value);
            }
        }
    }
}