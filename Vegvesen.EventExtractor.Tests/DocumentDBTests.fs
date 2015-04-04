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
        let getEvents (table : Table.CloudTable) = 
            table |> getAllTableEntities |> Seq.truncate 1

        let table = account.EventXmlTableClient.GetTableReference(containerName)
        populateEventJsonStore account containerName (getEvents table) saveEventAsJsonToDocumentStore

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should populate DocumentDB with 1000 JSON documents`` (containerName) =
        let account = AccountInfo()
        let getEvents (table : Table.CloudTable) = 
            table |> getAllTableEntities |> Seq.truncate 1000

        let table = account.EventXmlTableClient.GetTableReference(containerName)
        populateEventJsonStore account containerName (getEvents table) saveEventAsJsonToDocumentStore

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
