namespace Vegvesen.EventExtractor

open System.Collections.Generic
open System.IO
open System.Text
open System.Xml

module XmlParser =

    let parseNodes (stream : Stream) (elementName : string) (idElementName : string) =

        let rec readNode (reader : XmlReader) (nodes : Dictionary<string, string>) =

            let rec readId (reader : XmlReader) =
                if reader.Read() then
                    match reader.NodeType with
                    | XmlNodeType.Element when reader.Name = idElementName -> 
                        reader.GetAttribute("id")
                    | _ -> readId reader
                else
                    failwith "Unable to find id"

            match reader.NodeType with
            | XmlNodeType.Element when reader.Name = elementName -> 
                if elementName = idElementName then
                    let id = reader.GetAttribute("id")
                    let node = reader.ReadOuterXml()
                    nodes.Add(id, node)
                else
                    let node = reader.ReadOuterXml()
                    use innerReader = XmlReader.Create(StringReader(node))
                    let id = readId innerReader
                    nodes.Add(id, node)

            | _ -> reader.Read() |> ignore
            if (reader.ReadState <> ReadState.EndOfFile) then
                readNode reader nodes

        let nodes = Dictionary<string, string>()
        use xr = new XmlTextReader(stream)
        readNode xr nodes
        nodes |> Seq.map (|KeyValue|)

    let extractDiff xsold xsnew =

        let isNewOrUpdated (id,node) xs =
            let x = xs |> Seq.tryFind (fun (xid,xnode) -> id = xid)
            match x with
            | Some(yid,ynode) when node <> ynode -> true
            | None -> true
            | _ -> false

        xsnew |> Seq.filter (fun x -> isNewOrUpdated x xsold)