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

module ElasticsearchTests =

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should create Elasticsearch index with mappings`` (containerName) =
        let account = AccountInfo()
        createElasticStoreIndex account containerName

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should populate Elasticsearch with first JSON document`` (containerName) =
        let account = AccountInfo()

        let table = account.EventXmlTableClient.GetTableReference(containerName)
        populateEventJsonStoreAsync 
            account containerName 
            (getAllTableEntitiesAsync table) (Seq.truncate 1) 
            saveEventAsJsonToElasticStoreAsync
        |> Async.RunSynchronously

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should populate Elasticsearch with 1000 JSON documents`` (containerName) =
        let account = AccountInfo()

        let table = account.EventXmlTableClient.GetTableReference(containerName)
        populateEventJsonStoreAsync 
            account containerName 
            (getAllTableEntitiesAsync table) (Seq.truncate 1000) 
            saveEventAsJsonToElasticStoreAsync
        |> Async.RunSynchronously

    [<Literal>]
    let FromDate = "2015-01-01"
    [<Literal>]
    let ToDate = "2015-04-01"

    [<TestCase("getmeasurementweathersitetable", FromDate, ToDate, 1)>]
    [<TestCase("getmeasuredweatherdata", FromDate, ToDate, 10)>]
    [<TestCase("getpredefinedtraveltimelocations", FromDate, ToDate, 1)>]
    [<TestCase("getsituation", "2015-01-01", "2015-04-01", 10)>]
    [<TestCase("gettraveltimedata", FromDate, ToDate, 10)>]
    let ``should populate Elasticsearch with JSON documents for the selected date interval`` (containerName, fromDate, toDate, n) =
        let account = AccountInfo()

        let fromKey = Utils.timeToRowKey ((DateTime.Parse toDate).AddDays(1.))
        let toKey = Utils.timeToRowKey (DateTime.Parse fromDate)
        let table = account.EventXmlTableClient.GetTableReference(containerName)

        let takeEachNth n xs =
            xs 
            |> Seq.mapi (fun i x -> (i, x)) 
            |> Seq.filter (fun (i, x) -> i % n = 0)
            |> Seq.map (fun (i, x) -> x) 

        let idtable = account.EventXmlTableClient.GetTableReference("eventoriginids")
        let ids = getTableEntitiesByQueryAsync idtable <| sprintf "PartitionKey eq '%s'" containerName
                |> Async.RunSynchronously
        printfn "Found %d ids" (Seq.length ids)
        ids
        |> Seq.map (fun x -> x.RowKey)
        |> Seq.map (fun id -> 
            let getEventsAsync = getTableEntitiesByQueryAsync table <| sprintf 
                                    "PartitionKey eq '%s' and RowKey ge '%s' and RowKey lt '%s'" 
                                    id fromKey toKey
            populateEventJsonStoreAsync 
                account containerName 
                getEventsAsync (takeEachNth n) 
                saveEventAsJsonToElasticStoreAsync)
        |> Async.Parallel
        |> Async.RunSynchronously