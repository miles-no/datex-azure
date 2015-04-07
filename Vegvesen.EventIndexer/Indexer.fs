namespace Vegvesen.EventIndexer

open System
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client
open Newtonsoft.Json.Linq

open Vegvesen.Model
open Vegvesen.EventExtractor

[<AutoOpen>]
module Indexer =

//    let populateEventJsonStore (account : AccountInfo) containerName
//            (events : seq<Table.DynamicTableEntity>) 
//            (writeJsonToStore : AccountInfo -> string -> JObject -> unit) =
//        let eventBlobContainer = account.EventXmlBlobClient.GetContainerReference(containerName + "-events")
//        let coordBlobContainer = account.CoordinateJsonBlobClient.GetContainerReference(containerName + "-events-json-coordinates")
//
//        printfn "Populating events from container %s" containerName
//
//        events
//        |> Seq.map (fun x -> getEventXmlAndConvertToJson x eventBlobContainer containerName)
//        |> Seq.concat
//        |> PSeq.iter (fun (json, coordinates, eventSourceId, eventTime) ->
//            try
//                writeJsonToStore account containerName json
////                    let (eventSourceId, timeId) = Utils.parseJsonId json
////                    match coordinates with
////                    | None -> ()
////                    | Some(coordinates) -> saveEventCoordinatesAsJsonToBlobStore coordBlobContainer coordinates eventSourceId eventTime)
//            with
//            | ex -> printfn "%s" ex.Message)

    let populateEventJsonStoreAsync (account : AccountInfo) containerName
            (events : Async<seq<Table.DynamicTableEntity>>) 
            (eventFilter : seq<Table.DynamicTableEntity> -> seq<Table.DynamicTableEntity>)
            (writeJsonToStoreAsync : AccountInfo -> string -> JObject -> Async<unit>) =
        let eventBlobContainer = account.EventXmlBlobClient.GetContainerReference(containerName + "-events")
        let coordBlobContainer = account.CoordinateJsonBlobClient.GetContainerReference(containerName + "-events-json-coordinates")

        printfn "Populating events from container %s" containerName

        let getEventsAndCoordinates (events : seq<Table.DynamicTableEntity>) =
            events
            |> eventFilter
            |> Seq.map (fun x -> getEventXmlAndConvertToJsonAsync x eventBlobContainer containerName)
            |> Async.concat

        let saveEventAndCoordinates json coordinates eventSourceId eventTime = 
            async {
                let! result = writeJsonToStoreAsync account containerName json
//                let (eventSourceId, timeId) = Utils.parseJsonId json
//                match coordinates with
//                | None -> ()
//                | Some(coordinates) -> saveEventCoordinatesAsJsonToBlobStore coordBlobContainer coordinates eventSourceId eventTime
                ()
            }

        async {
            let! results = 
                events |> Async.map
                    (fun x -> x |> getEventsAndCoordinates |> Async.map 
                                (fun y -> y |> Seq.map(fun (json, coordinates, eventSourceId, eventTime) -> 
                                    saveEventAndCoordinates json coordinates eventSourceId eventTime
                                )))

            let! results = results |> Async.map (fun x -> x |> Async.Parallel)
            return! results
        }
