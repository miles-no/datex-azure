namespace Vegvesen.EventExtractor.Tests

open System
open System.Configuration
open System.Threading.Tasks
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client
open Microsoft.Azure.Documents.Linq
open NUnit.Framework
open FsUnit
open Vegvesen.EventExtractor

module DocumentDBTests =

    let getStorageAccounts =
        System.Net.ServicePointManager.DefaultConnectionLimit <- 100
        System.Threading.ThreadPool.SetMinThreads(100, 100) |> ignore
        let eventAccount = CloudStorageAccount.Parse(ConfigurationManager.ConnectionStrings.Item("VegvesenEventStorage").ConnectionString)
        let documentConnectionString = ConfigurationManager.ConnectionStrings.Item("VegvesenEventDocumentDB").ConnectionString
        let endpointKV = documentConnectionString.Split(';') |> Seq.head
        let authorizaionKV = documentConnectionString.Split(';') |> Seq.last
        let documentUri = endpointKV.Substring(string("EndpointUrl=").Length)
        let documentPassword = authorizaionKV.Substring(string("AuthorizationKey=").Length)
        (eventAccount, documentUri, documentPassword)

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    //[<TestCase("getcctvsitetable")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should populate DocumentDB with JSON documents`` (containerName) =
        let (eventAccount, documentUri, documentPassword) = getStorageAccounts
        let tableClient = eventAccount.CreateCloudTableClient()
        let blobClient = eventAccount.CreateCloudBlobClient()
        let table = tableClient.GetTableReference(containerName)
        let blobContainer = blobClient.GetContainerReference(containerName + "-events")
        let documentClient = DocumentClient(Uri(documentUri), documentPassword)

        let query = Table.TableQuery<Table.DynamicTableEntity>()
        table.ExecuteQuery(query) 
            |> Seq.map (fun x -> 
                        let blob = blobContainer.GetBlockBlobReference(x.PartitionKey + "/" + x.RowKey)
                        let text = blob.DownloadText()
                        convertXmlToJson text)
            |> Seq.iter (fun x -> 
                        documentClient 
                        |> getDatabase "Events" 
                        |> getCollection containerName 
                        |> createDocument x 
                        |> ignore)
