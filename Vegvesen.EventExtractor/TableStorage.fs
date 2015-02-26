namespace Vegvesen.EventExtractor

open System
open System.Text
open System.IO
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage

[<AutoOpen>]
module TableStorage =

    let saveEvent (table: Table.CloudTable) (blobContainer : Blob.CloudBlobContainer) item (time : DateTime) =
        let (id, content : string) = item
        let rowKey = (DateTime.MaxValue.Ticks - time.Ticks + 1L).ToString("d19")

        let entity = Table.DynamicTableEntity(id, rowKey)
        entity.Properties.Add("PublicationTime", Table.EntityProperty(Nullable<DateTime>(time)))

        let blobName = id + "/" + rowKey
        let blob = blobContainer.GetBlockBlobReference(blobName);
        blob.Properties.ContentType <-"application/xml";
        blob.UploadText(content)
        
        let operation = Table.TableOperation.InsertOrReplace(entity)
        table.Execute(operation) |> ignore
