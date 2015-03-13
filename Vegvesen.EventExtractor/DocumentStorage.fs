namespace Vegvesen.EventExtractor

open System
open System.Text
open System.IO
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client
open Microsoft.Azure.Documents.Linq

[<AutoOpen>]
module DocumentStorage =

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

    let createDocument (document : obj) ((client : DocumentClient), (db : Database), (collection : DocumentCollection)) =
        client.CreateDocumentAsync(collection.DocumentsLink, document).Result
