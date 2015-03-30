namespace Vegvesen.EventIndexer

open System
open Microsoft.WindowsAzure.Storage
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client
open Newtonsoft.Json.Linq

open Vegvesen.Model
open Vegvesen.EventExtractor

[<AutoOpen>]
module Indexer =

    let populateEventJsonStore (account : AccountInfo) containerName maxEventCount (writeJsonToStore : AccountInfo -> string -> JObject -> unit) =
        let table = account.EventXmlTableClient.GetTableReference(containerName)
        let eventBlobContainer = account.EventXmlBlobClient.GetContainerReference(containerName + "-events")
        let coordBlobContainer = account.CoordinateJsonBlobClient.GetContainerReference(containerName + "-coordinates")

        printfn "Populating up to %d events, container %s" maxEventCount containerName

        let query = Table.TableQuery<Table.DynamicTableEntity>()
        table.ExecuteQuery(query) 
            |> Seq.truncate maxEventCount
            |> Seq.map (fun x -> getEventXmlAndConvertToJson x eventBlobContainer containerName)
            |> Seq.concat
            |> Seq.iter (fun (json, coordinates, eventSourceId, eventTime) -> 
                writeJsonToStore account containerName json
                let (eventSourceId, timeId) = Utils.parseJsonId json
                match coordinates with
                | None -> ()
                | Some(coordinates) -> saveEventCoordinatesAsJsonToBlobStore coordBlobContainer coordinates eventSourceId eventTime)

