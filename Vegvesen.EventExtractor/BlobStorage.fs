namespace Vegvesen.EventExtractor

open System
open System.IO
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage

[<AutoOpen>]
module BlobStorage =

    [<Literal>]
    let MaxDirLevels = 3

    let enumerateDirectories (container : Blob.CloudBlobContainer) (startDir : string) =

        let listDirs (dir : string) =
            match dir.Split('/').Length with
            | MaxDirLevels -> 
                Seq.empty
            | _ -> 
                container.ListBlobs(dir + (if dir = "" then "" else "/"))
                |> Seq.filter (fun x -> x :? Blob.CloudBlobDirectory)
                |> Seq.map (fun x -> (x :?> Blob.CloudBlobDirectory).Prefix)
                |> Seq.map (fun x -> (x.Substring(0, x.Length-1)))

        let rec listDirsRecursive (dirs : string seq) =
            match dirs |> List.ofSeq with
            | [] -> Seq.empty
            | head :: tail ->
                let dirs = listDirs head
                           |> Seq.filter (fun x -> x >= startDir.Substring(0, Math.Min(x.Length, startDir.Length)))
                seq {
                    yield! listDirsRecursive dirs
                    yield! dirs
                    yield! listDirsRecursive tail
                }

        listDirsRecursive [""]

    let enumerateBlobs (container : Blob.CloudBlobContainer) (startBlob : string) =
        let listBlobs (dir : string) = 
            container.ListBlobs(dir + (if dir = "" then "" else "/"))
            |> Seq.filter (fun x -> x :? Blob.CloudBlockBlob)
            |> Seq.map (fun x -> (x :?> Blob.CloudBlockBlob).Name)
            |> Seq.filter (fun x -> x > startBlob)

        let startDir = startBlob.Substring(0, startBlob.LastIndexOf('/'))
        enumerateDirectories container startDir
        |> List.ofSeq
        |> PSeq.collect (fun x -> listBlobs x)
        |> PSeq.toList
        |> List.sort

    let getBlobContent (container : Blob.CloudBlobContainer) eventSourceId timestampId =
        let blob = container.GetBlockBlobReference(eventSourceId + "/" + timestampId)
        blob.DownloadText()
