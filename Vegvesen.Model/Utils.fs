namespace Vegvesen.Model

open System
open System.IO
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage
open Newtonsoft.Json
open Newtonsoft.Json.Linq

module Utils =

    let getXmlEventsContainerName (baseContainerName : string) =
        baseContainerName + "-events"

    let getJsonEventsContainerName (baseContainerName : string) =
        baseContainerName + "-events-json"

    let getJsonCoordinatesContainerName (baseContainerName : string) =
        baseContainerName + "-events-json-coordinates"

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

    let parseJsonId (json : JObject) =
        let id = json.["id"].ToString()
        let pos = id.LastIndexOf('_')
        let eventSourceId = id.Substring(0, pos)
        let timeId = id.Substring(pos + 1)
        (eventSourceId, timeId)
