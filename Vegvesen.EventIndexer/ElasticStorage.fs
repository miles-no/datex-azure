namespace Vegvesen.EventIndexer

open System
open System.IO
open Microsoft.WindowsAzure.Storage
open Newtonsoft.Json.Linq
open Elasticsearch.Net
open Elasticsearch.Net.Connection

open Vegvesen.Model
open Vegvesen.EventExtractor

[<AutoOpen>]
module ElasticStorage =

    let validateResponse (response : ElasticsearchResponse<DynamicDictionary>) =
        match response.Success with
        | true -> ()
        | false -> raise (response.OriginalException)

    let createElasticStoreIndex (account : AccountInfo) containerName =
        use reader = new StreamReader(sprintf "%s.index.json" containerName)
        let settings = reader.ReadToEnd()
        account.EventElasticClient.IndicesCreate(containerName, settings) |> validateResponse

    let saveEventAsJsonToElasticStore (account : AccountInfo) containerName (document : JObject) =
        let id = document.["id"].ToString()
        account.EventElasticClient.Index(containerName, containerName, id, document.ToString()) |> validateResponse
