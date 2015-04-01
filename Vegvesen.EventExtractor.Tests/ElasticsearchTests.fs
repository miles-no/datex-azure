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
        populateEventJsonStore account containerName 1 saveEventAsJsonToElasticStore

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("getsituation")>]
    [<TestCase("gettraveltimedata")>]
    let ``should populate Elasticsearch with 1000 JSON documents`` (containerName) =
        let account = AccountInfo()
        populateEventJsonStore account containerName 1000 saveEventAsJsonToElasticStore
