namespace Vegvesen.EventExtractor

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.IO
open System.Text
open System.Xml
open Newtonsoft.Json

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

    let convertXmlToJson xml (time : DateTime) =
        let doc = XmlDocument()
        doc.LoadXml(xml)
        JsonConvert.SerializeXmlNode(doc.DocumentElement, Formatting.None, true)