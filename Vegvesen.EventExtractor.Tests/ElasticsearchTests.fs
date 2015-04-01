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
        let getEvents (table : Table.CloudTable) = 
            let query = Table.TableQuery<Table.DynamicTableEntity>()
            table.ExecuteQuery(query) |> Seq.truncate 1

        populateEventJsonStore account containerName getEvents saveEventAsJsonToElasticStore

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should populate Elasticsearch with 1000 JSON documents`` (containerName) =
        let account = AccountInfo()
        let getEvents (table : Table.CloudTable) = 
            let query = Table.TableQuery<Table.DynamicTableEntity>()
            table.ExecuteQuery(query) |> Seq.truncate 1

        populateEventJsonStore account containerName getEvents saveEventAsJsonToElasticStore

    [<TestCase("getmeasurementweathersitetable", "2014-12-31", "2015-01-01")>]
    [<TestCase("getmeasuredweatherdata", "2014-12-31", "2015-01-01")>]
    [<TestCase("getpredefinedtraveltimelocations", "2014-12-31", "2015-01-01")>]
    [<TestCase("getsituation", "2014-12-31", "2015-01-01")>]
    [<TestCase("gettraveltimedata", "2014-12-31", "2015-01-01")>]
    let ``should populate Elasticsearch with JSON documents for the selected date interval`` (containerName, fromDate, toDate) =
        let account = AccountInfo()

        let fromKey = Utils.timeToRowKey ((DateTime.Parse toDate).AddDays(1.))
        let toKey = Utils.timeToRowKey (DateTime.Parse fromDate)

        let getEvents (table : Table.CloudTable) =
            let idtable = account.EventXmlTableClient.GetTableReference("eventoriginids")
            let query = Table.TableQuery<Table.DynamicTableEntity>()
            query.FilterString <- sprintf "PartitionKey eq '%s'" containerName
            let ids = idtable.ExecuteQuery(query) |> Seq.toList
            printfn "Found %d ids" (List.length ids)
            ids
            |> Seq.map (fun x -> x.RowKey)
            |> Seq.map (fun id -> 
                        let query = Table.TableQuery<Table.DynamicTableEntity>()
                        query.FilterString <- sprintf "PartitionKey eq '%s' and RowKey ge '%s' and RowKey lt '%s'" 
                            id fromKey toKey
                        let results = table.ExecuteQuery(query) |> Seq.toList
                        printfn "Found %d results for id %s" (List.length results) id
                        results)
            |> Seq.concat

        populateEventJsonStore account containerName getEvents saveEventAsJsonToElasticStore
