namespace Vegvesen.EventExtractor

open System
open System.IO
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage

module Utils =

    let timeToRowKey (time : DateTime) =
        (DateTime.MaxValue.Ticks - time.Ticks + 1L).ToString("d19")

    let rowKeyToTime (rowKey : string) =        
        DateTime(DateTime.MaxValue.Ticks - Int64.Parse(rowKey) + 1L)

    let timeToId (time : DateTime) =
        time.ToString("s")

    let parseDocumentId (documentId : string) =
        let splitPos = documentId.LastIndexOf '_'
        let eventSourceId = documentId.Substring(0, splitPos-1)
        let timeId = documentId.Substring(splitPos+1)
        (eventSourceId, timeId)
