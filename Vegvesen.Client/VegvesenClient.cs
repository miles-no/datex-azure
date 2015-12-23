using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace Vegvesen.Client
{
    public class Result<T> where T : class
    {
        public T Content { get; set; }
        public DateTimeOffset? LastModified { get; set; }

        public Result(T content, DateTimeOffset? lastModified = null)
        {
            this.Content = content;
            this.LastModified = lastModified;
        }
    }

    public class VegvesenClient
    {
        private const string BaseUrl = "https://www.vegvesen.no/ws/no/vegvesen/veg/trafikkpublikasjon/";
        private const string IfModifiedSinceHeader = "If-Modified-Since";
        private const string LastModifiedHeader = "Last-Modified";

        private readonly string _userName;
        private readonly string _password;

        public static Dictionary<string, string> ServiceUrls = new Dictionary<string, string>()
        {
            {"GetMeasurementWeatherSiteTable", BaseUrl + "vaer/1/GetMeasurementWeatherSiteTable"},
            {"GetMeasuredWeatherData", BaseUrl + "vaer/1/GetMeasuredWeatherData"},
            {"GetCCTVSiteTable", BaseUrl + "kamera/1/GetCCTVSiteTable"},
            {"GetPredefinedTravelTimeLocations", BaseUrl + "reisetid/1/GetPredefinedTravelTimeLocations"},
            {"GetTravelTimeData", BaseUrl + "reisetid/1/GetTravelTimeData"},
            {"GetSituation", BaseUrl + "trafikk/1/GetSituation"},
        };

        public VegvesenClient(string connectionString)
        {
            var items = connectionString.Split(';');
            if (items.Length < 2)
                throw new ArgumentException("Invalid connection string.", "connectionString");

            foreach (var item in items)
            {
                var kv = item.Split('=');
                if (kv.First() == "UserName")
                    _userName = kv.Last();
                else if (kv.First() == "Password")
                    _password = kv.Last();
            }
        }

        public Task<Result<string>> GetDataAsStringAsync(string serviceUrl, DateTimeOffset? sinceTime)
        {
            return GetDataAsync<string>(serviceUrl, sinceTime);
        }

        public Task<Result<Stream>> GetDataAsStreamAsync(string serviceUrl, DateTimeOffset? sinceTime)
        {
            return GetDataAsync<Stream>(serviceUrl, sinceTime);
        }

        private async Task<Result<T>> GetDataAsync<T>(string serviceUrl, DateTimeOffset? sinceTime)
            where T : class
        {
            var handler = new HttpClientHandler
            {
                Credentials = new NetworkCredential(_userName, _password),
                PreAuthenticate = true
            };

            var client = new HttpClient(handler);
            var request = new HttpRequestMessage(HttpMethod.Get, serviceUrl);
            if (sinceTime.HasValue)
            {
                request.Headers.IfModifiedSince = sinceTime.Value;
            }
            var response = await client.SendAsync(request);

            if (response.StatusCode == HttpStatusCode.NotModified)
            {
                return new Result<T>(null, sinceTime);
            }

            if (!response.IsSuccessStatusCode)
                throw new WebException(string.Format("Error retrieving data from {0}, status code: {1}, reason: {2}",
                    serviceUrl, response.StatusCode, response.ReasonPhrase));

            return await CreateResultAsync<T>(response.Content, response.Content.Headers.LastModified);
        }

        private async Task<Result<T>> CreateResultAsync<T>(HttpContent content, DateTimeOffset? lastModified)
            where T : class
        {
            if (typeof(T) == typeof(string))
                return new Result<string>(await content.ReadAsStringAsync(), lastModified) as Result<T>;
            else if (typeof(T) == typeof(Stream))
                return new Result<Stream>(await content.ReadAsStreamAsync(), lastModified) as Result<T>;

            throw new InvalidOperationException(string.Format("Unable to create result of type {0}", typeof(T).Name));
        }
    }
}
