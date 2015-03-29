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
        AccountInfo()

    let getStorageContainers containerName clearContainers =
        let account = getStorageAccounts
        let containerName = containerName

        let sourceBlobContainer = account.SourceXmlBlobClient.GetContainerReference(containerName)
        let table = account.EventXmlTableClient.GetTableReference(containerName)
        let eventBlobContainer = account.EventXmlBlobClient.GetContainerReference(containerName + "-events")

        if clearContainers then
            table.DeleteIfExists() |> ignore
            eventBlobContainer.DeleteIfExists() |> ignore
            Task.Delay(1000) |> Async.AwaitIAsyncResult |> Async.Ignore |> Async.RunSynchronously
        table.CreateIfNotExists() |> ignore
        eventBlobContainer.CreateIfNotExists() |> ignore
        (sourceBlobContainer, table, eventBlobContainer)

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

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    //[<TestCase("getcctvsitetable")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should update 10 events`` (containerName) =
        let account = getStorageAccounts
        updateServiceEvents account containerName 10

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    //[<TestCase("getcctvsitetable")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should populate EventOriginIds table`` (containerName) =
        let account = getStorageAccounts
        let table = account.EventXmlTableClient.GetTableReference(containerName)
        let idtable = account.EventXmlTableClient.GetTableReference("eventoriginids")
        let query = Table.TableQuery<Table.DynamicTableEntity>()
        table.ExecuteQuery(query) 
            |> Seq.groupBy (fun x -> x.PartitionKey) 
            |> Seq.iter (fun (x,_) -> 
                let entity = Table.DynamicTableEntity(containerName, x)
                let operation = Table.TableOperation.InsertOrReplace(entity)
                idtable.Execute(operation) |> ignore)
