namespace Vegvesen.EventIndexer

open System
open Microsoft.WindowsAzure.Storage
open Newtonsoft.Json.Linq
open Elasticsearch.Net
open Elasticsearch.Net.Connection

open Vegvesen.Model
open Vegvesen.EventExtractor

[<AutoOpen>]
module ElasticStorage =

    let saveEventAsJsonToElasticStore (account : AccountInfo) containerName (document : JObject) =
        let node = new Uri("http://es-vm-01-pocllmw4.cloudapp.net:9200")
        let config = ConnectionConfiguration(node)
        let client = ElasticsearchClient(config)
        client.Index(containerName, containerName, document.ToString()) |> ignore
