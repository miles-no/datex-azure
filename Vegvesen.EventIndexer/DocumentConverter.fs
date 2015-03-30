namespace Vegvesen.EventIndexer

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Xml.Linq
open Microsoft.WindowsAzure.Storage
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client
open Newtonsoft.Json
open Newtonsoft.Json.Linq

open Vegvesen.Model
open Vegvesen.EventExtractor

[<AutoOpen>]
module DocumentConverter =

    [<Literal>]
    let PublicationTimeElementName = "PublicationTime"

    let populateEventDocumentStore (account : AccountInfo) containerName maxEventCount =
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
                saveEventAsJsonToDocumentStore account.EventDocumentClient containerName json
                let (eventSourceId, timeId) = Utils.parseJsonId json
                match coordinates with
                | None -> ()
                | Some(coordinates) -> saveEventCoordinatesAsJsonToBlobStore coordBlobContainer coordinates eventSourceId eventTime)

    let populateEventJsonStore (account : AccountInfo) containerName maxEventCount =
        let table = account.EventXmlTableClient.GetTableReference(containerName)
        let eventBlobContainer = account.EventXmlBlobClient.GetContainerReference(containerName + "-events")
        let jsonBlobContainer = account.EventJsonBlobClient.GetContainerReference(containerName + "-events-json")
        let coordBlobContainer = account.CoordinateJsonBlobClient.GetContainerReference(containerName + "-events-json-coordinates")

        printfn "Populating up to %d events, container %s" maxEventCount containerName

        let query = Table.TableQuery<Table.DynamicTableEntity>()
        table.ExecuteQuery(query) 
            |> Seq.truncate maxEventCount
            |> Seq.map (fun x -> getEventXmlAndConvertToJson x eventBlobContainer containerName)
            |> Seq.concat
            |> Seq.iter (fun (json, coordinates, eventSourceId, eventTime) -> 
                saveEventAsJsonToBlobStore jsonBlobContainer containerName json
                let (eventSourceId, timeId) = Utils.parseJsonId json
                match coordinates with
                | None -> ()
                | Some(coordinates) -> saveEventCoordinatesAsJsonToBlobStore coordBlobContainer coordinates eventSourceId eventTime)

    let loadDocumentAttachments (account : AccountInfo) containerName (document : Document) =
        match containerName with
        | "getsituation" | "getpredefinedtraveltimelocations" -> 
            let blobContainer = account.CoordinateJsonBlobClient.GetContainerReference(containerName + "-coordinates")
            let blobName = document.Id
            let blob = blobContainer.GetBlockBlobReference(blobName);
            let coordinates = blob.DownloadText()
            (document, Some coordinates)
        | _ -> (document, None)
