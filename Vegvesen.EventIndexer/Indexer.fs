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

    let populateEventJsonStore (account : AccountInfo) containerName
            (getEvents : (Table.CloudTable -> seq<Table.DynamicTableEntity>)) 
            (writeJsonToStore : AccountInfo -> string -> JObject -> unit) =
        let table = account.EventXmlTableClient.GetTableReference(containerName)
        let eventBlobContainer = account.EventXmlBlobClient.GetContainerReference(containerName + "-events")
        let coordBlobContainer = account.CoordinateJsonBlobClient.GetContainerReference(containerName + "-coordinates")

        printfn "Populating events from container %s" containerName

        table
            |> getEvents
            |> Seq.map (fun x -> getEventXmlAndConvertToJson x eventBlobContainer containerName)
            |> Seq.concat
            |> Seq.iter (fun (json, coordinates, eventSourceId, eventTime) -> 
                writeJsonToStore account containerName json)
//                let (eventSourceId, timeId) = Utils.parseJsonId json
//                match coordinates with
//                | None -> ()
//                | Some(coordinates) -> saveEventCoordinatesAsJsonToBlobStore coordBlobContainer coordinates eventSourceId eventTime)
