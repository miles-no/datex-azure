namespace Vegvesen.EventExtractor.Tests

open System
open System.Configuration
open System.Threading.Tasks
open Microsoft.FSharp.Collections
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client
open Microsoft.Azure.Documents.Linq
open NUnit.Framework
open FsUnit

open Vegvesen.Model
open Vegvesen.EventExtractor
open Vegvesen.EventIndexer

module DocumentDBTests =

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should populate DocumentDB with first JSON document`` (containerName) =
        let account = AccountInfo()
        populateEventJsonStore account containerName 1 saveEventAsJsonToDocumentStore

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should populate DocumentDB with 1000 JSON documents`` (containerName) =
        let account = AccountInfo()
        populateEventJsonStore account containerName 1000 saveEventAsJsonToDocumentStore

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should read first document from DocumentDB`` (containerName) =
        let account = AccountInfo()
        let json = account.EventDocumentClient
                |> getDatabase DatabaseName 
                |> getCollection containerName 
                |> getDocuments
                |> Seq.head
        printfn "JSON: %s" (json.ToString())

        let (_,coordinates) = loadDocumentAttachments account containerName json
        if coordinates.IsSome then
            printfn "Coordinates: %s" (coordinates.ToString())
        else
            ()
