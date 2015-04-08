namespace Vegvesen.EventExtractor

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Xml.Linq
open Microsoft.WindowsAzure.Storage
open Newtonsoft.Json
open Newtonsoft.Json.Linq

open Vegvesen.Model

[<AutoOpen>]
module JsonConverter =

    let rec groupCoordinatesPairwise (collection : double list) =
        match collection with
        | [] -> []
        | x :: y :: tail -> JArray([x; y]) :: groupCoordinatesPairwise tail
        | _ -> []

    let coordinatesToArray (text : string) =
        text.Split([|' '; ','; ';'; '\r'; '\n'|]) 
        |> Array.filter (fun x -> not (String.IsNullOrWhiteSpace(x)))
        |> Array.map (fun x -> System.Double.Parse x)

    let rec convertCoordinates (token : JToken) =
        token
        |> Seq.iter (fun x ->
                    if x :? JProperty && (x :?> JProperty).Name = "coordinates" then
                        let prop = (x :?> JProperty)
                        let value = coordinatesToArray (prop.Value.ToString()) |> List.ofArray |> groupCoordinatesPairwise
                        prop.Value <- JArray(value)
                    else
                        convertCoordinates x)

    let extractCoordinates containerName (json : JObject) =

        let rec flattenProperties (json : JToken) =
            seq {
                for x in json do
                    match x with
                    | xp when (xp :? JProperty) ->
                        let prop = xp :?> JProperty
                        match prop.Value with
                        | y when (y :? JObject) ->
                            yield prop
                            yield! flattenProperties (y :?> JObject)
                        | y when (y :? JArray) ->
                            yield prop
                            yield! flattenProperties (y :?> JArray)
                        | y when (y :? JValue) ->                    
                            yield prop
                        | _ -> ()
                    | _ -> ()
            }

        let extractPropertyNode propName (props : seq<JProperty>) =
            props |> Seq.tryPick (fun x -> 
                    match x.Name with
                    | n when n = propName ->
                        let v = x.Value
                        x.Value <- null
                        Some v
                    | _ -> None)

        match containerName with
        | "getsituation" | "getpredefinedtraveltimelocations" -> 
            match json |> flattenProperties |> extractPropertyNode "linearExtension" with
            | Some coordinates -> (json, Some coordinates)
            | None -> (json, None)
        | _ -> (json, None)

    let (|Bool|Int|Float|Date|String|) s = 
        match System.Boolean.TryParse s with
        | true, v -> Bool v
        | _ ->
        match System.Int64.TryParse s with
        | true, v -> Int v
        | _ ->
        match System.Double.TryParse s with
        | true, v -> Float v
        | _ ->
        match System.DateTime.TryParse s with
        | true, v -> Date v
        | _ -> String s

    let rec transform (rename: string -> string) (remove: string -> bool) (changeType : JToken -> JToken) (json: JToken) =
        match json with
        | :? JProperty as prop -> 
            let name = rename prop.Name
            let value = changeType prop.Value
            let cont = transform rename remove changeType value
            new JProperty(name, cont :> obj) :> JToken
        | :? JArray as arr ->
            let cont = arr |> Seq.map (transform rename remove changeType)
            new JArray(cont) :> JToken
        | :? JObject as o ->
            let cont = o.Properties() |> Seq.filter (fun p -> not (remove p.Name)) |> Seq.map (transform rename remove changeType)
            new JObject(cont) :> JToken
        | _ -> json

    let mapPropertyName propName =
        match propName with
        | "latitude" -> "lat"
        | "longitude" -> "lon"
        | _ -> propName

    let shouldRemoveProperty propName =
        match propName with
        | "@xmlns" -> true
        | _ -> false

    let changePropertyType (value : JToken) =
        match value with
        | :? JValue as jval ->
            match (jval.Value.ToString()) with
            | Bool v -> JValue(v) :> JToken
            | Int v -> JValue(v) :> JToken
            | Float v -> JValue(v) :> JToken
            | _ -> value
        | _ -> value

    let postprocessJson containerName eventSourceId index (eventTime : DateTime) (publicationTime : DateTime) (json : JObject) =
        let id = match containerName with
                    | "getsituation" -> sprintf "%s_%d" eventSourceId (index+1)
                    | _ -> eventSourceId
        json.AddFirst(JProperty("publicationTime", publicationTime))
        json.AddFirst(JProperty("id", sprintf "%s_%s" id (Utils.timeToId eventTime)))
        convertCoordinates json
        json |> transform mapPropertyName shouldRemoveProperty changePropertyType :?> JObject

    let convertXmlToJson containerName eventSourceId (eventTime : DateTime) (publicationTime : DateTime) xml =
        XElement.Parse(xml) 
        |> removeNamespaces
        |> preprocessXml containerName eventSourceId
        |> List.map (fun x -> JsonConvert.SerializeXNode(x, Formatting.None, true) |> JObject.Parse)
        |> List.mapi (fun i x -> postprocessJson containerName eventSourceId i eventTime publicationTime x)
        |> List.map (fun x -> extractCoordinates containerName x)
        |> List.map (fun (json, coordinates) -> (json, coordinates, eventSourceId, eventTime))

    let getEventXmlAndConvertToJson (entity : Table.DynamicTableEntity) (eventBlobContainer : Blob.CloudBlobContainer) containerName =
        getBlobContent eventBlobContainer entity.PartitionKey entity.RowKey 
        |> convertXmlToJson containerName entity.PartitionKey 
            (Utils.rowKeyToTime entity.RowKey) 
            (entity.Properties.["PublicationTime"].DateTime).Value

    let getEventXmlAndConvertToJsonAsync (entity : Table.DynamicTableEntity) (eventBlobContainer : Blob.CloudBlobContainer) containerName =
        async {
            let! blob = (getBlobContentAsync eventBlobContainer entity.PartitionKey entity.RowKey)
            return blob |> convertXmlToJson containerName entity.PartitionKey 
                        (Utils.rowKeyToTime entity.RowKey) 
                        (entity.Properties.["PublicationTime"].DateTime).Value
        }
