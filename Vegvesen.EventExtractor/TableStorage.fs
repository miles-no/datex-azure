namespace Vegvesen.EventExtractor

open System
open System.Text
open System.IO
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

open Vegvesen.Model

[<AutoOpen>]
module TableStorage =

    let saveEvent (table: CloudTable) (blobContainer : Blob.CloudBlobContainer) event (time : DateTime) =
        let (id, content : string) = event
        let rowKey = Utils.timeToRowKey time

        let entity = DynamicTableEntity(id, rowKey)
        entity.Properties.Add("PublicationTime", EntityProperty(Nullable<DateTime>(time)))

        let blobName = id + "/" + rowKey
        let blob = blobContainer.GetBlockBlobReference(blobName)
        blob.Properties.ContentType <-"application/xml"
        let operation = TableOperation.InsertOrReplace(entity)

        blob.UploadText(content)
        table.Execute(operation) |> ignore

    let saveEventOriginId (idTable : CloudTable) containerName event =
        let (id, _) = event
        let entity = DynamicTableEntity(containerName, id)
        let operation = TableOperation.InsertOrReplace(entity)
        idTable.Execute(operation) |> ignore

    let executeTableQuery (table : CloudTable) (query : TableQuery<DynamicTableEntity>) =
        table.ExecuteQuery(query)

    let executeTableQueryAsync (table : CloudTable) (query : TableQuery<DynamicTableEntity>) =

        let rec executeQuerySegmentedAsync (table : CloudTable) 
            (query : TableQuery<DynamicTableEntity>) (continuationToken : TableContinuationToken) =
            async {
                let! result = table.ExecuteQuerySegmentedAsync(query, continuationToken) |> Async.AwaitTask
                let rows = result.Results |> List.ofSeq
                match result.ContinuationToken with
                | null -> return rows |> Seq.ofList
                | token -> 
                    let! result = executeQuerySegmentedAsync table query result.ContinuationToken
                    return result
            }

        executeQuerySegmentedAsync table query null

    let getTableEntitiesByQueryAsync (table : CloudTable) queryString =
        let query = TableQuery<DynamicTableEntity>()
        query.FilterString <- queryString
        executeTableQueryAsync table query

    let getAllTableEntitiesAsync (table : CloudTable) =
        executeTableQueryAsync table (TableQuery<DynamicTableEntity>())
