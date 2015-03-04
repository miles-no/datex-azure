namespace Vegvesen.EventExtractor.Tests

open System
open System.Configuration
open System.Threading.Tasks
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage
open NUnit.Framework
open FsUnit
open Vegvesen.EventExtractor

module Tests =

    let getStorageAccount =
        System.Net.ServicePointManager.DefaultConnectionLimit <- 100
        System.Threading.ThreadPool.SetMinThreads(100, 100) |> ignore
        let connectionString = ConfigurationManager.ConnectionStrings.Item("AzureWebJobsStorage").ConnectionString
        CloudStorageAccount.Parse(connectionString)

    let getStorageContainers containerName clearContainers =
        let account = getStorageAccount
        let containerName = containerName
        let blobClient = account.CreateCloudBlobClient()
        let rawBlobContainer = blobClient.GetContainerReference(containerName)
        let tableClient = account.CreateCloudTableClient()
        let table = tableClient.GetTableReference(containerName)
        let eventBlobContainer = blobClient.GetContainerReference(containerName + "-events")
        if clearContainers then
            table.DeleteIfExists() |> ignore
            eventBlobContainer.DeleteIfExists() |> ignore
            Task.Delay(1000) |> Async.AwaitIAsyncResult |> Async.Ignore |> Async.RunSynchronously
        table.CreateIfNotExists() |> ignore
        eventBlobContainer.CreateIfNotExists() |> ignore
        (rawBlobContainer, table, eventBlobContainer)

    [<Test>]
    let ``should return five first blobs`` () =

        let (blobContainer, table, _) = getStorageContainers "getmeasuredweatherdata" false

        updateLastExtractedBlobName blobContainer None |> ignore
        let blobs = enumerateBlobs blobContainer "2015/01/01/000000" |> Seq.take 5 |> Array.ofSeq
        blobs.[0] |> should equal "2015/01/01/000001"
        blobs.[1] |> should equal "2015/01/01/000501"
        blobs.[2] |> should equal "2015/01/01/001503"
        blobs.[3] |> should equal "2015/01/01/002205"
        blobs.[4] |> should equal "2015/01/01/003000"

    [<Test>]
    let ``should extract events from the first blob`` () =

        let (blobContainer, table, _) = getStorageContainers "getmeasuredweatherdata" false

        updateLastExtractedBlobName blobContainer None |> ignore
        let blobName = enumerateBlobs blobContainer "2015/01/01/000000" |> Seq.head
        
        let (publicationTime, events) = extractServiceEvents blobContainer "" blobName
        publicationTime |> should equal <| DateTime.Parse("2015-01-01 00:59:59.897")
        events |> Seq.length |> should equal 301

    [<Test>]
    [<Ignore>]
    let ``should create events from five first blobs`` () =

        let (rawBlobContainer, table, eventBlobContainer) = getStorageContainers "getmeasuredweatherdata" true

        updateLastExtractedBlobName rawBlobContainer None |> ignore
        let blobs = enumerateBlobs rawBlobContainer "2015/01/01/000000" |> Seq.take 5 |> List.ofSeq
        let blobPairs = ("", List.head blobs) :: (getPairwise blobs)
        blobPairs |> List.length |> should equal 5
        
        blobPairs 
        |> PSeq.map (fun (last, next) -> extractServiceEvents rawBlobContainer last next)
        |> PSeq.iter (fun (publicationTime, events) -> 
                    (events |> PSeq.iter (fun event -> saveEvent table eventBlobContainer event publicationTime)))

        updateLastExtractedBlobName rawBlobContainer (Some (blobs |> Seq.last))

        let lastBlobName = getLastExtractedBlobName rawBlobContainer
        lastBlobName |> Option.get |> should equal "2015/01/01/003000"

    [<Test>]
    [<Ignore>]
    let ``should create events from next ten blobs`` () =

        let (rawBlobContainer, table, eventBlobContainer) = getStorageContainers "getmeasuredweatherdata" false

        let lastBlobName = Option.get (getLastExtractedBlobName rawBlobContainer)
        let blobs = enumerateBlobs rawBlobContainer lastBlobName |> Seq.take 10 |> List.ofSeq
        let blobPairs = (lastBlobName, List.head blobs) :: (getPairwise blobs)
        blobPairs |> List.length |> should equal 10
        
        blobPairs 
        |> PSeq.map (fun (last, next) -> extractServiceEvents rawBlobContainer last next)
        |> PSeq.iter (fun (publicationTime, events) -> 
                    (events |> PSeq.iter (fun event -> saveEvent table eventBlobContainer event publicationTime)))

        updateLastExtractedBlobName rawBlobContainer (Some (blobs |> Seq.last))

        let newLastBlobName = getLastExtractedBlobName rawBlobContainer
        newLastBlobName |> Option.get |> should be (greaterThan lastBlobName)

    [<Test>]
    [<Ignore>]
    let ``should create events from the first large blob`` () =

        let (rawBlobContainer, table, eventBlobContainer) = getStorageContainers "getsituation" true

        updateLastExtractedBlobName rawBlobContainer None |> ignore
        let blobName = enumerateBlobs rawBlobContainer "2015/01/01/000000" |> Seq.head
        
        let (publicationTime, events) = extractServiceEvents rawBlobContainer "" blobName
        events |> Seq.iter (fun event -> saveEvent table eventBlobContainer event publicationTime)

        updateLastExtractedBlobName rawBlobContainer (Some blobName)

        let lastBlobName = getLastExtractedBlobName rawBlobContainer
        lastBlobName |> Option.get |> should equal blobName

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    //[<TestCase("getcctvsitetable")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("gettraveltimedata")>]
    let ``should update 1000 events`` (containerName) =
        updateServiceEvents getStorageAccount containerName 1000

    [<TestCase("getsituation")>]
    let ``should update 100 large blob events`` (containerName) =
        updateServiceEvents getStorageAccount containerName 100
