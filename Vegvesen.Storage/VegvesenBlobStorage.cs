using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Vegvesen.Storage
{
    public class VegvesenBlobStorage : IVegvesenStorage
    {
        private readonly CloudBlobClient _blobClient;

        public VegvesenBlobStorage(string connectionString)
        {
            var account = CloudStorageAccount.Parse(connectionString);
            _blobClient = account.CreateCloudBlobClient();
        }

        public async Task<DateTimeOffset?> GetLastModifiedTime(string serviceName)
        {
            var containerName = serviceName.ToLower();
            var blobContainer = _blobClient.GetContainerReference(containerName);
            if (blobContainer.Exists())
            {
                var blobName = "LastModified";
                var blob = blobContainer.GetBlockBlobReference(blobName);
                if (blob.Exists())
                {
                    var text = await blob.DownloadTextAsync();
                    DateTimeOffset lastModifiedTime;
                    if (DateTimeOffset.TryParse(text, out lastModifiedTime))
                    {
                        return lastModifiedTime;
                    }
                }
            }
            return null;
        }

        public async Task SaveEntryDataAsync(string serviceName, Stream stream, DateTimeOffset lastModifiedTime)
        {
            var containerName = serviceName.ToLower();
            var blobContainer = _blobClient.GetContainerReference(containerName);
            await blobContainer.CreateIfNotExistsAsync();
            var blobName = lastModifiedTime.LocalDateTime.ToString("yyyy/MM/dd/HHmmss");
            var blob = blobContainer.GetBlockBlobReference(blobName);
            if (!blob.Exists())
            {
                await blob.UploadFromStreamAsync(stream);
            }

            blobName = "LastModified";
            blob = blobContainer.GetBlockBlobReference(blobName);
            await blob.UploadTextAsync(lastModifiedTime.ToString("s"));
        }

        public Task<IEnumerable<string>> GetEntryNamesAsync(string serviceName)
        {
            IEnumerable<string> strings = new List<string>();
            return Task.FromResult(strings);
        }

        public Task<string> GetEntryDataAsync(string serviceName, string entryName)
        {
            return Task.FromResult(string.Empty);
        }
    }
}