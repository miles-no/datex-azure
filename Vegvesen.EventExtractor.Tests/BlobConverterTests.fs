namespace Vegvesen.EventExtractor.Tests

open System
open System.Configuration
open System.Threading.Tasks
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage
open NUnit.Framework
open FsUnit
open Vegvesen.EventExtractor

module BlobConverterTests =

    let getStorageAccounts =
        System.Net.ServicePointManager.DefaultConnectionLimit <- 100
        System.Threading.ThreadPool.SetMinThreads(100, 100) |> ignore
        let sourceAccount = CloudStorageAccount.Parse(ConfigurationManager.ConnectionStrings.Item("AzureWebJobsStorage").ConnectionString)
        let eventAccount = CloudStorageAccount.Parse(ConfigurationManager.ConnectionStrings.Item("VegvesenEventStorage").ConnectionString)
        (sourceAccount, eventAccount)

    let getStorageContainers containerName clearContainers =
        let (sourceAccount, eventAccount) = getStorageAccounts
        let containerName = containerName

        let sourceBlobClient = sourceAccount.CreateCloudBlobClient()
        let tableClient = eventAccount.CreateCloudTableClient()
        let eventBlobClient = eventAccount.CreateCloudBlobClient()

        let sourceBlobContainer = sourceBlobClient.GetContainerReference(containerName)
        let table = tableClient.GetTableReference(containerName)
        let eventBlobContainer = eventBlobClient.GetContainerReference(containerName + "-events")

        if clearContainers then
            table.DeleteIfExists() |> ignore
            eventBlobContainer.DeleteIfExists() |> ignore
            Task.Delay(1000) |> Async.AwaitIAsyncResult |> Async.Ignore |> Async.RunSynchronously
        table.CreateIfNotExists() |> ignore
        eventBlobContainer.CreateIfNotExists() |> ignore
        (sourceBlobContainer, table, eventBlobContainer)

    [<Test>]
    let ``should delete event storage`` () =

        let (_, eventAccount) = getStorageAccounts
        let tableClient = eventAccount.CreateCloudTableClient()
        let eventBlobClient = eventAccount.CreateCloudBlobClient()

        for containerName in blobContainers do
            let table = tableClient.GetTableReference(containerName.ToLower())
            let eventBlobContainer = eventBlobClient.GetContainerReference(containerName.ToLower() + "-events")
            table.DeleteIfExists() |> ignore
            eventBlobContainer.DeleteIfExists() |> ignore

    [<Test>]
    let ``should return five first blobs`` () =

        let (sourceBlobContainer, table, _) = getStorageContainers "getmeasuredweatherdata" false

        let blobs = enumerateBlobs sourceBlobContainer "2015/01/01/000000" |> Seq.take 5 |> Array.ofSeq
        blobs.[0] |> should equal "2015/01/01/000001"
        blobs.[1] |> should equal "2015/01/01/000501"
        blobs.[2] |> should equal "2015/01/01/001503"
        blobs.[3] |> should equal "2015/01/01/002205"
        blobs.[4] |> should equal "2015/01/01/003000"

    [<Test>]
    let ``should extract events from the first blob`` () =

        let (sourceBlobContainer, table, _) = getStorageContainers "getmeasuredweatherdata" false

        let blobName = enumerateBlobs sourceBlobContainer "2015/01/01/000000" |> Seq.head
        
        let (publicationTime, events) = extractServiceEvents sourceBlobContainer "" blobName
        publicationTime |> should equal <| DateTime.Parse("2015-01-01 00:59:59.897")
        events |> Seq.length |> should equal 301

//    [<Test>]
//    [<Ignore>]
//    let ``should create events from five first blobs`` () =
//
//        let (sourceBlobContainer, table, eventBlobContainer) = getStorageContainers "getmeasuredweatherdata" true
//
//        updateLastExtractedBlobName eventBlobContainer None |> ignore
//        let blobs = enumerateBlobs sourceBlobContainer "2015/01/01/000000" |> Seq.take 5 |> List.ofSeq
//        let blobPairs = ("", List.head blobs) :: (getPairwise blobs)
//        blobPairs |> List.length |> should equal 5
//        
//        blobPairs 
//        |> PSeq.map (fun (last, next) -> extractServiceEvents sourceBlobContainer last next)
//        |> PSeq.iter (fun (publicationTime, events) -> 
//                    (events |> PSeq.iter (fun event -> saveEvent table eventBlobContainer event publicationTime)))
//
//        updateLastExtractedBlobName eventBlobContainer (Some (blobs |> Seq.last))
//
//        let lastBlobName = getLastExtractedBlobName eventBlobContainer
//        lastBlobName |> Option.get |> should equal "2015/01/01/003000"
//
//    [<Test>]
//    [<Ignore>]
//    let ``should create events from next ten blobs`` () =
//
//        let (sourceBlobContainer, table, eventBlobContainer) = getStorageContainers "getmeasuredweatherdata" false
//
//        let lastBlobName = Option.get (getLastExtractedBlobName eventBlobContainer)
//        let blobs = enumerateBlobs sourceBlobContainer lastBlobName |> Seq.take 10 |> List.ofSeq
//        let blobPairs = (lastBlobName, List.head blobs) :: (getPairwise blobs)
//        blobPairs |> List.length |> should equal 10
//        
//        blobPairs 
//        |> PSeq.map (fun (last, next) -> extractServiceEvents sourceBlobContainer last next)
//        |> PSeq.iter (fun (publicationTime, events) -> 
//                    (events |> PSeq.iter (fun event -> saveEvent table eventBlobContainer event publicationTime)))
//
//        updateLastExtractedBlobName eventBlobContainer (Some (blobs |> Seq.last))
//
//        let newLastBlobName = getLastExtractedBlobName eventBlobContainer
//        newLastBlobName |> Option.get |> should be (greaterThan lastBlobName)
//
//    [<Test>]
//    [<Ignore>]
//    let ``should create events from the first large blob`` () =
//
//        let (sourceBlobContainer, table, eventBlobContainer) = getStorageContainers "getsituation" true
//
//        updateLastExtractedBlobName eventBlobContainer None |> ignore
//        let blobName = enumerateBlobs sourceBlobContainer "2015/01/01/000000" |> Seq.head
//        
//        let (publicationTime, events) = extractServiceEvents sourceBlobContainer "" blobName
//        events |> Seq.iter (fun event -> saveEvent table eventBlobContainer event publicationTime)
//
//        updateLastExtractedBlobName eventBlobContainer (Some blobName)
//
//        let lastBlobName = getLastExtractedBlobName eventBlobContainer
//        lastBlobName |> Option.get |> should equal blobName

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    //[<TestCase("getcctvsitetable")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should update 10 events`` (containerName) =
        let (sourceAccount, eventAccount) = getStorageAccounts
        updateServiceEvents sourceAccount eventAccount containerName 10

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    //[<TestCase("getcctvsitetable")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should populate EventOriginIds table`` (containerName) =
        let (_, eventAccount) = getStorageAccounts
        let tableClient = eventAccount.CreateCloudTableClient()
        let table = tableClient.GetTableReference(containerName)
        let idtable = tableClient.GetTableReference("eventoriginids")
        let query = Table.TableQuery<Table.DynamicTableEntity>()
        table.ExecuteQuery(query) 
            |> Seq.groupBy (fun x -> x.PartitionKey) 
            |> Seq.iter (fun (x,_) -> 
                let entity = Table.DynamicTableEntity(containerName, x)
                let operation = Table.TableOperation.InsertOrReplace(entity)
                idtable.Execute(operation) |> ignore)
