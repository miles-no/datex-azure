using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Vegvesen.Storage
{
    public interface IVegvesenStorage
    {
        Task<DateTimeOffset?> GetLastModifiedTime(string serviceName);
        Task SaveEntryDataAsync(string serviceName, Stream stream, DateTimeOffset lastModifiedTime);
        Task<IEnumerable<string>> GetEntryNamesAsync(string serviceName);
        Task<string> GetEntryDataAsync(string serviceName, string entryName);
    }
}