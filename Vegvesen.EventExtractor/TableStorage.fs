namespace Vegvesen.EventExtractor

open System
open System.Text
open System.IO
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage

[<AutoOpen>]
module TableStorage =

    [<Literal>]
    let MaxStringSize = 0x8000
    let MaxPropertySize = 0x10000

    let saveEvent (table: Table.CloudTable) item (time : DateTime) =
        let (id, content : string) = item
        let rowKey = (DateTime.MaxValue.Ticks - time.Ticks).ToString("d19")

        let entity = Table.DynamicTableEntity(id, rowKey)
        entity.Properties.Add("PublicationTime", Table.EntityProperty(Nullable<DateTime>(time)))
        if content.Length <= MaxStringSize then
            entity.Properties.Add("Content", Table.EntityProperty(content))
        else
            let bytes = Encoding.UTF8.GetBytes(content)
            let bytesLength = bytes |> Array.length
            let numSegments = bytesLength / MaxPropertySize + 1
            entity.Properties.Add("Content", Table.EntityProperty(numSegments.ToString()))
            for i in 1 .. numSegments do 
                let propName = "Content" + i.ToString()
                let bytesOffset = (i-1) * MaxPropertySize
                let propSize = Math.Min(MaxPropertySize, bytesLength - bytesOffset)
                let propValue = Array.sub bytes bytesOffset propSize
                entity.Properties.Add(propName, Table.EntityProperty(propValue))
        
        let operation = Table.TableOperation.InsertOrReplace(entity)
        table.Execute(operation) |> ignore
