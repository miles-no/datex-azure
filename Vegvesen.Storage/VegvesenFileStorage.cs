using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Vegvesen.Storage
{
    public class VegvesenFileStorage : IVegvesenStorage
    {
        private readonly string _rootPath;

        public VegvesenFileStorage(string rootPath)
        {
            _rootPath = rootPath;
        }

        public async Task<DateTimeOffset?> GetLastModifiedTime(string serviceName)
        {
            var serviceDir = Path.Combine(_rootPath, serviceName);
            if (Directory.Exists(serviceDir))
            {
                var filename = Path.Combine(serviceDir, "LastModified.txt");
                if (File.Exists(filename))
                {
                    using (var reader = new StreamReader(filename))
                    {
                        var text = await reader.ReadToEndAsync();
                        DateTimeOffset lastModifiedTime;
                        if (DateTimeOffset.TryParse(text, out lastModifiedTime))
                        {
                            return lastModifiedTime;
                        }
                    }
                }
            }
            return null;
        }

        public async Task SaveEntryDataAsync(string serviceName, Stream stream, DateTimeOffset lastModifiedTime)
        {
            var serviceDir = Path.Combine(_rootPath, serviceName);
            if (!Directory.Exists(serviceDir))
            {
                Directory.CreateDirectory(serviceDir);
            }

            var filename = Path.Combine(serviceDir, string.Format("{0}.xml", lastModifiedTime.LocalDateTime.ToString("yyyyMMdd-HHmmss")));
            if (!File.Exists(filename))
            {
                using (var reader = new StreamReader(stream))
                {
                    using (var writer = new StreamWriter(filename))
                    {
                        await writer.WriteAsync(await reader.ReadToEndAsync());
                    }
                }
            }

            filename = Path.Combine(serviceDir, "LastModified.txt");
            using (var writer = new StreamWriter(filename))
            {
                await writer.WriteAsync(lastModifiedTime.ToString("s"));
            }
        }

        public Task<IEnumerable<string>> GetEntryNamesAsync(string serviceName)
        {
            return Task.FromResult(
                new DirectoryInfo(Path.Combine(_rootPath, serviceName))
                .GetFileSystemInfos("????????-??????.xml").Select(x => x.Name));
        }

        public async Task<string> GetEntryDataAsync(string serviceName, string entryName)
        {
            var serviceDir = Path.Combine(_rootPath, serviceName);
            var filename = Path.Combine(serviceDir, entryName);
            using (var reader = new StreamReader(filename))
            {
                return await reader.ReadToEndAsync();
            }
        }
    }
}