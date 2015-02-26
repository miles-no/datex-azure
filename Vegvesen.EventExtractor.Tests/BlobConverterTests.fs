namespace Vegvesen.EventExtractor.Tests

open System
open System.Configuration
open Microsoft.WindowsAzure.Storage
open NUnit.Framework
open FsUnit
open Vegvesen.EventExtractor

module Tests =

    let getStorageAccount =
        let connectionString = ConfigurationManager.ConnectionStrings.Item("AzureWebJobsStorage").ConnectionString
        CloudStorageAccount.Parse(connectionString)

    let getStorageContainers containerName =
        let account = getStorageAccount
        let containerName = containerName
        let blobClient = account.CreateCloudBlobClient()
        let rawBlobContainer = blobClient.GetContainerReference(containerName)
        let tableClient = account.CreateCloudTableClient()
        let table = tableClient.GetTableReference(containerName)
        table.CreateIfNotExists() |> ignore
        let eventBlobContainer = blobClient.GetContainerReference(containerName + "-events")
        eventBlobContainer.CreateIfNotExists() |> ignore
        (rawBlobContainer, table, eventBlobContainer)

    [<Test>]
    let ``should return five first blobs`` () =

        let (blobContainer, table, _) = getStorageContainers "getmeasuredweatherdata"

        updateLastExtractedBlobName blobContainer None |> ignore
        let blobs = enumerateBlobs blobContainer "2015/01/01/000000" |> Seq.take 5 |> Array.ofSeq
        blobs.[0] |> should equal "2015/01/01/000001"
        blobs.[1] |> should equal "2015/01/01/000501"
        blobs.[2] |> should equal "2015/01/01/001503"
        blobs.[3] |> should equal "2015/01/01/002205"
        blobs.[4] |> should equal "2015/01/01/003000"

    [<Test>]
    let ``should extract events from the first blob`` () =

        let (blobContainer, table, _) = getStorageContainers "getmeasuredweatherdata"

        updateLastExtractedBlobName blobContainer None |> ignore
        let blobName = enumerateBlobs blobContainer "2015/01/01/000000" |> Seq.head
        
        let (publicationTime, events) = extractServiceEvents blobContainer "" blobName
        publicationTime |> should equal <| DateTime.Parse("2015-01-01 00:59:59.897")
        events |> Seq.length |> should equal 301

    [<Test>]
    let ``should create events from five first blobs`` () =

        let (rawBlobContainer, table, eventBlobContainer) = getStorageContainers "getmeasuredweatherdata"

        updateLastExtractedBlobName rawBlobContainer None |> ignore
        let blobs = enumerateBlobs rawBlobContainer "2015/01/01/000000" |> Seq.take 5 |> List.ofSeq
        let blobPairs = ("", List.head blobs) :: (getPairwise blobs)
        blobPairs |> List.length |> should equal 5
        
        blobPairs 
        |> Seq.map (fun (last, next) -> extractServiceEvents rawBlobContainer last next)
        |> Seq.iter (fun (publicationTime, events) -> 
                    (events |> Seq.iter (fun event -> saveEvent table eventBlobContainer event publicationTime)))

        updateLastExtractedBlobName rawBlobContainer (Some (blobs |> Seq.last))

        let lastBlobName = getLastExtractedBlobName rawBlobContainer
        lastBlobName |> Option.get |> should equal "2015/01/01/003000"

    [<Test>]
    let ``should create events from the first large blob`` () =

        let (rawBlobContainer, table, eventBlobContainer) = getStorageContainers "getsituation"

        updateLastExtractedBlobName rawBlobContainer None |> ignore
        let blobName = enumerateBlobs rawBlobContainer "2015/01/01/000000" |> Seq.head
        
        let (publicationTime, events) = extractServiceEvents rawBlobContainer "" blobName
        events |> Seq.iter (fun event -> saveEvent table eventBlobContainer event publicationTime)

        updateLastExtractedBlobName rawBlobContainer (Some blobName)

        let lastBlobName = getLastExtractedBlobName rawBlobContainer
        lastBlobName |> Option.get |> should equal blobName
