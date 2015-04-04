namespace Vegvesen.EventExtractor

open System
open System.Text
open System.IO
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage

open Vegvesen.Model

[<AutoOpen>]
module TableStorage =

    let saveEvent (table: Table.CloudTable) (blobContainer : Blob.CloudBlobContainer) event (time : DateTime) =
        let (id, content : string) = event
        let rowKey = Utils.timeToRowKey time

        let entity = Table.DynamicTableEntity(id, rowKey)
        entity.Properties.Add("PublicationTime", Table.EntityProperty(Nullable<DateTime>(time)))

        let blobName = id + "/" + rowKey
        let blob = blobContainer.GetBlockBlobReference(blobName)
        blob.Properties.ContentType <-"application/xml"
        let operation = Table.TableOperation.InsertOrReplace(entity)

        blob.UploadText(content)
        table.Execute(operation) |> ignore

    let saveEventOriginId (idTable : Table.CloudTable) containerName event =
        let (id, _) = event
        let entity = Table.DynamicTableEntity(containerName, id)
        let operation = Table.TableOperation.InsertOrReplace(entity)
        idTable.Execute(operation) |> ignore

    let executeTableQuery (table : Table.CloudTable) (query : Table.TableQuery<Table.DynamicTableEntity>) =
        table.ExecuteQuery(query)

    let getTableEntitiesByQuery (table : Table.CloudTable) queryString =
        let query = Table.TableQuery<Table.DynamicTableEntity>()
        query.FilterString <- queryString
        executeTableQuery table query

    let getAllTableEntities (table : Table.CloudTable) =
        executeTableQuery table (Table.TableQuery<Table.DynamicTableEntity>())
