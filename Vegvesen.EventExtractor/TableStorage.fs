namespace Vegvesen.EventExtractor

open System
open System.IO
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage
open DigitallyCreated.FSharp.Azure.TableStorage

module TableStorage =

    type VegvesenEvent = { 
        [<PartitionKey>] Id: string
        [<RowKey>] TimeCode: string
        PublicationTime: DateTime
        Content: string
    }

    let saveEvent (tableClient : Table.CloudTableClient) tableName item (time : DateTime) =
        let (id, content) = item
        let timeCode = (DateTime.MaxValue.Ticks - time.Ticks).ToString("d19")
        let event = { Id = id; TimeCode = timeCode; PublicationTime = time; Content = content }

        do event |> Insert |> (inTable tableClient tableName) |> ignore
