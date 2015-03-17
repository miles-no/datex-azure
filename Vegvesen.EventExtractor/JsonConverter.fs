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
        let rec extractCoordinatesNode (token : JToken) =
            token
            |> Seq.reduce (fun acc x ->
                        if x :? JProperty && (x :?> JProperty).Name = "linearExtension" then
                            let prop = (x :?> JProperty)
                            let value = prop.Value
                            prop.Value <- null
                            value
                        else
                            extractCoordinatesNode x)

        match containerName with
        | "getsituation" | "getpredefinedtraveltimelocations" -> (json, Some(extractCoordinatesNode json))
        | _ -> (json, None)

    let postprocessJson containerName eventSourceId index (publicationTime : DateTime) (json : JObject) =
        let id = match containerName with
                    | "getsituation" -> sprintf "%s_%d" eventSourceId (index+1)
                    | _ -> eventSourceId
        json.AddFirst(JProperty("publicationTime", publicationTime))
        json.AddFirst(JProperty("id", sprintf "%s_%s" id (publicationTime.ToString("s"))))
        convertCoordinates json
