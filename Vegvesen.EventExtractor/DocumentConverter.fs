namespace Vegvesen.EventExtractor

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

[<AutoOpen>]
module DocumentConverter =

    [<Literal>]
    let PublicationTimeElementName = "PublicationTime"

    let convertXmlToJson containerName eventSourceId (eventTime : DateTime) (publicationTime : DateTime) xml =
        XElement.Parse(xml) 
        |> removeNamespaces
        |> preprocessXml containerName eventSourceId
        |> List.map (fun x -> JsonConvert.SerializeXNode(x, Formatting.None, true) |> JObject.Parse)
        |> List.mapi (fun i x -> postprocessJson containerName eventSourceId i eventTime publicationTime x)
        |> List.map (fun x -> extractCoordinates containerName x)
        |> List.map (fun (json, coordinates) -> (json, coordinates, eventSourceId, eventTime))

    let getEventXmlAndConvertToJson (entity : Table.DynamicTableEntity) (eventBlobContainer : Blob.CloudBlobContainer) containerName =
        getBlobContent eventBlobContainer entity.PartitionKey entity.RowKey 
        |> convertXmlToJson containerName entity.PartitionKey 
            (Utils.rowKeyToTime entity.RowKey) 
            (entity.Properties.["PublicationTime"].DateTime).Value

    let populateEventDocumentStore (eventAccount : CloudStorageAccount) (documentUri : string) (documentPassword : string) containerName maxEventCount =
        let tableClient = eventAccount.CreateCloudTableClient()
        let blobClient = eventAccount.CreateCloudBlobClient()
        let table = tableClient.GetTableReference(containerName)
        let eventBlobContainer = blobClient.GetContainerReference(containerName + "-events")
        use documentClient = new DocumentClient(Uri(documentUri), documentPassword)
        let coordBlobContainer = blobClient.GetContainerReference(containerName + "-coordinates")

        printfn "Populating up to %d events, container %s" maxEventCount containerName

        let query = Table.TableQuery<Table.DynamicTableEntity>()
        table.ExecuteQuery(query) 
            |> Seq.truncate maxEventCount
            |> Seq.map (fun x -> getEventXmlAndConvertToJson x eventBlobContainer containerName)
            |> Seq.concat
            |> Seq.iter (fun (json, coordinates, eventSourceId, eventTime) -> 
                saveEventAsJsonToDocumentStore documentClient containerName json
                let (eventSourceId, timeId) = parseDocumentId json
                match coordinates with
                | None -> ()
                | Some(coordinates) -> saveEventCoordinatesAsJsonToBlobStore coordBlobContainer coordinates eventSourceId eventTime)

    let populateEventJsonStore (eventAccount : CloudStorageAccount) containerName maxEventCount =
        let tableClient = eventAccount.CreateCloudTableClient()
        let eventBlobClient = eventAccount.CreateCloudBlobClient()
        let table = tableClient.GetTableReference(containerName)
        let eventBlobContainer = eventBlobClient.GetContainerReference(containerName + "-events")
        let jsonBlobContainer = eventBlobClient.GetContainerReference(containerName + "-events-json")
        let coordBlobContainer = eventBlobClient.GetContainerReference(containerName + "-events-json-coordinates")

        printfn "Populating up to %d events, container %s" maxEventCount containerName

        let query = Table.TableQuery<Table.DynamicTableEntity>()
        table.ExecuteQuery(query) 
            |> Seq.truncate maxEventCount
            |> Seq.map (fun x -> getEventXmlAndConvertToJson x eventBlobContainer containerName)
            |> Seq.concat
            |> Seq.iter (fun (json, coordinates, eventSourceId, eventTime) -> 
                saveEventAsJsonToBlobStore jsonBlobContainer containerName json
                let (eventSourceId, timeId) = parseDocumentId json
                match coordinates with
                | None -> ()
                | Some(coordinates) -> saveEventCoordinatesAsJsonToBlobStore coordBlobContainer coordinates eventSourceId eventTime)

    let loadDocumentAttachments (blobClient : Blob.CloudBlobClient) containerName (document : Document) =
        match containerName with
        | "getsituation" | "getpredefinedtraveltimelocations" -> 
            let blobContainer = blobClient.GetContainerReference(containerName + "-coordinates")
            let blobName = document.Id
            let blob = blobContainer.GetBlockBlobReference(blobName);
            let coordinates = blob.DownloadText()
            (document, Some coordinates)
        | _ -> (document, None)
