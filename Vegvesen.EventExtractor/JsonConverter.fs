namespace Vegvesen.EventExtractor

open System
open System.Collections.Generic
open System.IO
open System.Text
open Newtonsoft.Json
open Newtonsoft.Json.Linq

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

        let rec flattenProperties (json : JObject) =
            seq {
                for x in json do
                    match x with
                    | xp when (xp :? JProperty) ->
                        let prop = xp :?> JProperty
                        match prop.Value with
                        | y when (y :? JObject) ->
                            yield prop
                            yield! flattenProperties (y :?> JObject)
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

    let postprocessJson containerName eventSourceId index (eventTime : DateTime) (publicationTime : DateTime) (json : JObject) =
        let id = match containerName with
                    | "getsituation" -> sprintf "%s_%d" eventSourceId (index+1)
                    | _ -> eventSourceId
        json.AddFirst(JProperty("publicationTime", publicationTime))
        json.AddFirst(JProperty("id", sprintf "%s_%s" id (Utils.timeToId eventTime)))
        convertCoordinates json
        json
