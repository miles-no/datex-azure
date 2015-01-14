using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Vegvesen.Client.Tests
{
    public class VegvesenClientTests
    {
        private readonly string _connectionString;

        public VegvesenClientTests()
        {
            _connectionString = ConfigurationManager.ConnectionStrings["VegvesenDatex"].ConnectionString;
        }

        [Fact]
        public async Task GetSingleServiceDataAsString_SinceNull_ShouldReturnDataAndLastModified()
        {
            var client = new VegvesenClient(_connectionString);
            var result = await client.GetDataAsStringAsync(VegvesenClient.ServiceUrls.First(), null);

            Assert.NotNull(result.Content);
            Assert.NotNull(result.LastModified);
        }

        [Fact]
        public async Task GetSingleServiceDataAsStream_SinceNull_ShouldReturnDataAndLastModified()
        {
            var client = new VegvesenClient(_connectionString);
            var result = await client.GetDataAsStreamAsync(VegvesenClient.ServiceUrls.First(), null);

            Assert.NotNull(result.Content);
            Assert.NotNull(result.LastModified);
        }

        [Fact]
        public async Task GetSingleServiceDataAsString_SinceLastFetch_ShouldReturnNoData()
        {
            var client = new VegvesenClient(_connectionString);
            var result = await client.GetDataAsStringAsync(VegvesenClient.ServiceUrls.First(), null);
            var lastModified = result.LastModified;

            result = await client.GetDataAsStringAsync(VegvesenClient.ServiceUrls.First(), lastModified);
            Assert.Null(result.Content);
            Assert.Equal(lastModified, result.LastModified);
        }

        [Fact]
        public async Task GetAllServiceDataAsString_LastMunute_ShouldReturnDataAndLastModified()
        {
            var client = new VegvesenClient(_connectionString);

            var sw = new Stopwatch();
            foreach (var serviceUrl in VegvesenClient.ServiceUrls)
            {
                sw.Start();
                var sinceTime = DateTimeOffset.Now.AddMinutes(-1);
                var result = await client.GetDataAsStringAsync(serviceUrl, sinceTime);
                sw.Stop();

                Assert.NotNull(result.LastModified);

                Console.WriteLine("{0}: {1} bytes, last modified {2}, elapsed {3}", 
                    serviceUrl, result.Content == null ? 0 : result.Content.Length, result.LastModified, sw.Elapsed);
            }
        }

        [Fact]
        public async Task GetAllServiceDataAsStream_SinceNull_ShouldReturnDataAndLastModified()
        {
            var client = new VegvesenClient(_connectionString);

            var sw = new Stopwatch();
            foreach (var serviceUrl in VegvesenClient.ServiceUrls)
            {
                sw.Start();
                var result = await client.GetDataAsStreamAsync(serviceUrl, null);
                sw.Stop();

                Assert.NotNull(result.Content);
                Assert.NotNull(result.LastModified);

                Console.WriteLine("{0}: last modified {1}, elapsed {2}",
                    serviceUrl, result.LastModified, sw.Elapsed);
            }
        }
    }
}
