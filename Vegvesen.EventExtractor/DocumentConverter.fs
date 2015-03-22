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
        |> List.map (fun x -> JsonConvert.SerializeXNode(x, Formatting.Indented, true) |> JObject.Parse)
        |> List.mapi (fun i x -> postprocessJson containerName eventSourceId i eventTime publicationTime x)
        |> List.map (fun x -> extractCoordinates containerName x)
        |> List.map (fun (json, node) -> (json, node, eventSourceId, eventTime))

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
            |> Seq.map (fun x -> 
                getBlobContent eventBlobContainer x.PartitionKey x.RowKey 
                |> convertXmlToJson containerName x.PartitionKey 
                    (Utils.rowKeyToTime x.RowKey) 
                    (x.Properties.["PublicationTime"].DateTime).Value)
            |> Seq.concat
            |> Seq.iter (fun (json, node, eventSourceId, eventTime) -> 
                saveDocument documentClient containerName json
                match node with
                | None -> ()
                | Some(node) ->
                    coordBlobContainer.CreateIfNotExists() |> ignore
                    let blobName = eventSourceId + "/" + (Utils.timeToId eventTime)
                    let blob = coordBlobContainer.GetBlockBlobReference(blobName);
                    blob.Properties.ContentType <-"application/json"
                    blob.UploadText(node.ToString()))

    let loadDocumentAttachments (blobClient : Blob.CloudBlobClient) containerName (document : Document) =
        match containerName with
        | "getsituation" | "getpredefinedtraveltimelocations" -> 
            let blobContainer = blobClient.GetContainerReference(containerName + "-coordinates")
            let blobName = document.Id
            let blob = blobContainer.GetBlockBlobReference(blobName);
            let coordinates = blob.DownloadText()
            (document, Some coordinates)
        | _ -> (document, None)
