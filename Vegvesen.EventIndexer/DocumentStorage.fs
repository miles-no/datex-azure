namespace Vegvesen.EventIndexer

open System
open System.Text
open System.Threading.Tasks
open System.IO
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client
open Microsoft.Azure.Documents.Linq
open Newtonsoft.Json.Linq

open Vegvesen.Model

[<AutoOpen>]
module DocumentStorage =

    [<Literal>]
    let DatabaseName = "Events"

    let getDatabase databaseName (client : DocumentClient) =
        let db = match client.CreateDatabaseQuery() |> Seq.tryFind(fun x -> x.Id = databaseName) with
                 | Some(db) -> db
                 | None -> client.CreateDatabaseAsync(Database(Id = databaseName)).Result.Resource
        (client, db)

    let getCollection collectionName ((client : DocumentClient), (db : Database)) =
        let collection = match client.CreateDocumentCollectionQuery(db.CollectionsLink) |> Seq.tryFind(fun x -> x.Id = collectionName) with
                         | Some(collection) -> collection
                         | None -> client.CreateDocumentCollectionAsync(db.CollectionsLink, DocumentCollection(Id = collectionName)).Result.Resource
        (client, db, collection)

    let getDocuments ((client : DocumentClient), (db : Database), (collection : DocumentCollection)) =
        client.CreateDocumentQuery(collection.DocumentsLink)

    let insertDocument (document : obj) ((client : DocumentClient), (db : Database), (collection : DocumentCollection)) =
        try
            client.CreateDocumentAsync(collection.DocumentsLink, document).Result
        with 
        | :? System.AggregateException ->
            printfn "Retrying after 2 sec..."
            Task.Delay 2000 |> Async.AwaitIAsyncResult |> Async.RunSynchronously |> ignore
            printfn "Retrying now..."
            let result = client.CreateDocumentAsync(collection.DocumentsLink, document).Result
            printfn "OK"
            result

    let saveEventAsJsonToDocumentStore (account : AccountInfo) containerName (document : JObject) =
        account.EventDocumentClient
        |> getDatabase DatabaseName 
        |> getCollection containerName 
        |> insertDocument document 
        |> ignore

    let saveEventAsJsonToDocumentStoreAsync (account : AccountInfo) containerName (document : JObject) =
        // TODO
        async {
            saveEventAsJsonToDocumentStore account containerName document
        }

    let loadDocumentAttachments (account : AccountInfo) containerName (document : Document) =
        match containerName with
        | "getsituation" | "getpredefinedtraveltimelocations" -> 
            let blobContainer = account.CoordinateJsonBlobClient.GetContainerReference(containerName + "-coordinates")
            let blobName = document.Id
            let blob = blobContainer.GetBlockBlobReference(blobName);
            let coordinates = blob.DownloadText()
            (document, Some coordinates)
        | _ -> (document, None)
