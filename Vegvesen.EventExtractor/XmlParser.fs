namespace Vegvesen.EventExtractor

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.IO
open System.Text
open System.Xml
open System.Xml.Linq
open Newtonsoft.Json
open Newtonsoft.Json.Linq

[<AutoOpen>]
module XmlParser =

    [<Literal>]
    let IdAttributeName = "id"
    [<Literal>]
    let PublicationTimeElementName = "publicationTime"

    let parseNodes (stream : Stream) (elementName : string) (idElementName : string) =

        let rec readNode (reader : XmlReader) (nodes : ConcurrentDictionary<string, string>) =

            let rec readId (reader : XmlReader) =
                if reader.Read() then
                    match reader.NodeType with
                    | XmlNodeType.Element when reader.Name = idElementName -> 
                        reader.GetAttribute(IdAttributeName)
                    | _ -> readId reader
                else
                    failwith "Unable to find id"

            match reader.NodeType with
            | XmlNodeType.Element when reader.Name = PublicationTimeElementName -> 
                let id = reader.Name
                let value = reader.ReadElementContentAsString()
                nodes.GetOrAdd(id, value) |> ignore
            | XmlNodeType.Element when reader.Name = elementName -> 
                if elementName = idElementName then
                    let id = reader.GetAttribute(IdAttributeName)
                    let node = reader.ReadOuterXml()
                    nodes.GetOrAdd(id, node) |> ignore
                else
                    let node = reader.ReadOuterXml()
                    use innerReader = XmlReader.Create(StringReader(node))
                    let id = readId innerReader
                    nodes.GetOrAdd(id, node) |> ignore
            | _ -> reader.Read() |> ignore

            if (reader.ReadState <> ReadState.EndOfFile) then
                readNode reader nodes

        let nodes = ConcurrentDictionary<string, string>()
        use xr = new XmlTextReader(stream)
        readNode xr nodes
        let publicationTime = DateTime.Parse(nodes.Item(PublicationTimeElementName))
        nodes.TryRemove(PublicationTimeElementName) |> ignore
        (publicationTime, nodes |> Seq.map (|KeyValue|))

    let extractDiff xsold xsnew =

        let isNewOrUpdated (id,node) xs =
            let x = xs |> Seq.tryFind (fun (xid,xnode) -> id = xid)
            match x with
            | Some(yid,ynode) when node <> ynode -> true
            | None -> true
            | _ -> false

        xsnew |> Seq.filter (fun x -> isNewOrUpdated x xsold)

    let removeNamespaces (doc : XElement) =
        for e in doc.DescendantsAndSelf() do
            for a in e.Attributes() do
                if a.IsNamespaceDeclaration then
                    a.Remove()
        doc

    let splitSituations eventSourceId (publicationTime : DateTime) (doc : XElement) =
        let situationHeader = XElement(doc.Name, 
                                XAttribute(doc.Attribute(XName.Get "version")), 
                                XAttribute(doc.Attribute(XName.Get "id")))
        situationHeader.Add(doc.Element(doc.Name.Namespace + "overallSeverity"))
        situationHeader.Add(doc.Element(doc.Name.Namespace + "headerInformation"))

        doc.Elements(doc.Name.Namespace + "situationRecord") 
        |> Seq.map (fun x -> 
                    x.AddFirst situationHeader
                    x)
        |> List.ofSeq

    let preprocessXml containerName eventSourceId (publicationTime : DateTime) (doc : XElement) =
        match containerName with
        | "getsituation" -> splitSituations eventSourceId publicationTime doc
        | _ -> [doc]

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

    let postprocessJson containerName eventSourceId index (publicationTime : DateTime) (json : JObject) =
        let id = match containerName with
                    | "getsituation" -> sprintf "%s_%d" eventSourceId (index+1)
                    | _ -> eventSourceId
        json.AddFirst(JProperty("publicationTime", publicationTime))
        json.AddFirst(JProperty("id", sprintf "%s_%s" id (publicationTime.ToString("s"))))
        convertCoordinates json
        json

    let convertXmlToJson containerName eventSourceId (publicationTime : DateTime) xml =
        XElement.Parse(xml) 
        |> removeNamespaces
        |> preprocessXml containerName eventSourceId publicationTime
        |> List.map (fun x -> JsonConvert.SerializeXNode(x, Formatting.Indented, true) |> JObject.Parse)
        |> List.mapi (fun i x -> postprocessJson containerName eventSourceId i publicationTime x)
