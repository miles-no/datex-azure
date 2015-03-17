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

    let convertXmlToJson containerName eventSourceId (publicationTime : DateTime) xml =
        XElement.Parse(xml) 
        |> removeNamespaces
        |> preprocessXml containerName eventSourceId publicationTime
        |> List.map (fun x -> JsonConvert.SerializeXNode(x, Formatting.Indented, true) |> JObject.Parse)
        |> List.mapi (fun i x -> postprocessJson containerName eventSourceId i publicationTime x)
        //|> List.map (fun x -> extractCoordinates containerName x)

    let populateEventDocumentStore (eventAccount : CloudStorageAccount) (documentUri : string) (documentPassword : string) containerName maxEventCount =
        let tableClient = eventAccount.CreateCloudTableClient()
        let blobClient = eventAccount.CreateCloudBlobClient()
        let table = tableClient.GetTableReference(containerName)
        let blobContainer = blobClient.GetContainerReference(containerName + "-events")
        use documentClient = new DocumentClient(Uri(documentUri), documentPassword)

        printfn "Populating up to %d events, container %s" maxEventCount containerName

        let query = Table.TableQuery<Table.DynamicTableEntity>()
        table.ExecuteQuery(query) 
            |> Seq.truncate maxEventCount
            |> Seq.map (fun x -> getBlobContent blobContainer x.PartitionKey x.RowKey 
                                |> convertXmlToJson containerName x.PartitionKey (x.Properties.Item("PublicationTime").DateTime.Value))
            |> Seq.concat
            |> Seq.iter (fun x -> saveDocument documentClient containerName x)
