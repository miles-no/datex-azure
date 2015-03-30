namespace Vegvesen.EventExtractor.Tests

open System
open System.Configuration
open System.Threading.Tasks
open Microsoft.WindowsAzure.Storage
open NUnit.Framework
open FsUnit

open Vegvesen.Model
open Vegvesen.EventExtractor
open Vegvesen.EventIndexer

module EventJsonTests =

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should convert XML to JSON`` (containerName) =
        let account = AccountInfo()
        let table = account.EventXmlTableClient.GetTableReference(containerName)
        let blobContainer = account.EventXmlBlobClient.GetContainerReference(containerName + "-events")

        let blobDir = blobContainer.ListBlobs() 
                        |> Seq.skipWhile (fun x -> not(x :? Blob.CloudBlobDirectory)) 
                        |> Seq.head :?> Blob.CloudBlobDirectory

        let blobRef = blobContainer.ListBlobs(blobDir.Prefix) 
                        |> Seq.head :?> Blob.CloudBlockBlob

        let blob = blobContainer.GetBlockBlobReference blobRef.Name
        let content = blob.DownloadText()
        let (json,node,_,_) = content 
                            |> convertXmlToJson containerName "123" DateTime.Now  DateTime.Now
                            |> List.head 
        printfn "JSON: %s" (json.ToString())
        if node.IsSome then printfn "Coordinates: %s" ((Option.get node).ToString())

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should populate blob storage with first JSON document`` (containerName) =
        let account = AccountInfo()
        populateEventBlobStore account containerName 1

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should populate blob storage with 100 JSON documents`` (containerName) =
        let account = AccountInfo()
        populateEventBlobStore account containerName 100
