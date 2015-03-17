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
    let ``should convert XML to JSON`` (containerName) =
        let (eventAccount, documentUri, documentPassword) = getStorageAccounts
        let tableClient = eventAccount.CreateCloudTableClient()
        let blobClient = eventAccount.CreateCloudBlobClient()
        let table = tableClient.GetTableReference(containerName)
        let blobContainer = blobClient.GetContainerReference(containerName + "-events")
        let documentClient = DocumentClient(Uri(documentUri), documentPassword)

        let blobDir = blobContainer.ListBlobs() 
                        |> Seq.skipWhile (fun x -> not(x :? Blob.CloudBlobDirectory)) 
                        |> Seq.head :?> Blob.CloudBlobDirectory

        let blobRef = blobContainer.ListBlobs(blobDir.Prefix) 
                        |> Seq.head :?> Blob.CloudBlockBlob

        let blob = blobContainer.GetBlockBlobReference blobRef.Name
        let content = blob.DownloadText()
        content |> convertXmlToJson containerName "123" DateTime.Now 
        |> List.head |> ignore

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    //[<TestCase("getcctvsitetable")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should populate DocumentDB with first JSON document`` (containerName) =
        let (eventAccount, documentUri, documentPassword) = getStorageAccounts
        populateEventDocumentStore eventAccount documentUri documentPassword containerName 1

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    //[<TestCase("getcctvsitetable")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should populate DocumentDB with 1000 JSON documents`` (containerName) =
        let (eventAccount, documentUri, documentPassword) = getStorageAccounts
        populateEventDocumentStore eventAccount documentUri documentPassword containerName 1000
