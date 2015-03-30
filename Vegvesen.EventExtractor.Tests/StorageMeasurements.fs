namespace Vegvesen.EventExtractor.Tests

open System
open System.Configuration
open System.Threading.Tasks
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage
open NUnit.Framework
open FsUnit

open Vegvesen.Model
open Vegvesen.EventExtractor

module StorageMeasurments =

    let getStorageContainers containerName clearContainers =
        let account = AccountInfo()

        let sourceBlobContainer = account.SourceXmlBlobClient.GetContainerReference(containerName)
        let table = account.EventXmlTableClient.GetTableReference(containerName)
        let eventBlobContainer = account.EventXmlBlobClient.GetContainerReference(containerName + "-events")

        (sourceBlobContainer, table, eventBlobContainer)

    let sizeInMegabytes (size : int64) = size / (1024L * 1024L)
    let sizeInKilobytes (size : int64) = size / 1024L

    [<TestCase("getmeasurementweathersitetable", "2015/01/01/")>]
    [<TestCase("getmeasurementweathersitetable", "2015/02/01/")>]
    [<TestCase("getmeasurementweathersitetable", "2015/03/01/")>]
    [<TestCase("getmeasuredweatherdata", "2015/01/01/")>]
    [<TestCase("getmeasuredweatherdata", "2015/02/01/")>]
    [<TestCase("getmeasuredweatherdata", "2015/03/01/")>]
    [<TestCase("getpredefinedtraveltimelocations", "2015/01/01/")>]
    [<TestCase("getpredefinedtraveltimelocations", "2015/02/01/")>]
    [<TestCase("getpredefinedtraveltimelocations", "2015/03/01/")>]
    [<TestCase("getsituation", "2015/01/01/")>]
    [<TestCase("getsituation", "2015/02/01/")>]
    [<TestCase("getsituation", "2015/03/01/")>]
    [<TestCase("gettraveltimedata", "2015/01/01/")>]
    [<TestCase("gettraveltimedata", "2015/02/01/")>]
    [<TestCase("gettraveltimedata", "2015/03/01/")>]
    let ``should list source blob count and sizes for one day`` (containerName, dir) =

        let (sourceBlobContainer, _, _) = getStorageContainers containerName false

        let (count, size) = sourceBlobContainer.ListBlobs(dir)
                            |> Seq.map (fun x -> x :?> Blob.CloudBlockBlob)
                            |> Seq.fold (fun (count, size) x -> (count + 1, size + x.Properties.Length)) (0, 0L)

        printfn "%d blobs of %i MB in %s for %s" count (sizeInMegabytes size) containerName dir |> ignore

    [<TestCase("getmeasurementweathersitetable")>]
    [<TestCase("getmeasuredweatherdata")>]
    [<TestCase("getpredefinedtraveltimelocations")>]
    [<TestCase("gettraveltimedata")>]
    let ``should list event blob count and sizes`` (containerName) =

        let (_, _, eventBlobContainer) = getStorageContainers containerName false

        let dirs = eventBlobContainer.ListBlobs("")
                |> Seq.filter (fun x -> x :? Blob.CloudBlobDirectory)
                |> Seq.map (fun x -> x :?> Blob.CloudBlobDirectory)
                |> Seq.map (fun x -> x.Prefix)

        printfn "%d event blob directories" (Seq.length dirs)

        let dir = dirs |> Seq.head
        let (count, size) = eventBlobContainer.ListBlobs(dir)
                            |> Seq.map (fun x -> x :?> Blob.CloudBlockBlob)
                            |> Seq.fold (fun (count, size) x -> (count + 1, size + x.Properties.Length)) (0, 0L)

        printfn "%d blobs of %i KB in %s" count (sizeInKilobytes size) containerName |> ignore

    [<TestCase("getsituation")>]
    let ``should list situation event blob count and sizes`` (containerName) =

        let (_, _, eventBlobContainer) = getStorageContainers containerName false

        let dirs = eventBlobContainer.ListBlobs("")
                |> Seq.filter (fun x -> x :? Blob.CloudBlobDirectory)
                |> Seq.map (fun x -> x :?> Blob.CloudBlobDirectory)
                |> Seq.map (fun x -> x.Prefix)
        let dirCount = Seq.length dirs

        printfn "%d event blob directories" dirCount

        let getBlobStats (blobs : seq<Blob.CloudBlockBlob>) =
            let count = Seq.length blobs
            (count, (int64)count * (Seq.head blobs).Properties.Length)

        let sampleCount = 50
        let (count, size) = dirs 
                            |> Seq.skip 50
                            |> Seq.truncate sampleCount
                            |> Seq.map (fun x -> eventBlobContainer.ListBlobs(x)
                                                |> Seq.map (fun x -> x :?> Blob.CloudBlockBlob) 
                                                |> getBlobStats)
                            |> Seq.fold (fun (count, size) (x, y) -> (count + x, size + y)) (0, 0L)
        let (count, size) = (count / sampleCount * dirCount, size * (int64)(dirCount / sampleCount)) 

        printfn "%d blobs of %i KB in %s" count (sizeInKilobytes (size / (int64)count)) containerName |> ignore

    [<TestCase("getsituation")>]
    let ``should list situation event blob count and sizes for one day`` (containerName) =

        let (_, _, eventBlobContainer) = getStorageContainers containerName false

        let dirs = eventBlobContainer.ListBlobs("")
                |> Seq.filter (fun x -> x :? Blob.CloudBlobDirectory)
                |> Seq.map (fun x -> x :?> Blob.CloudBlobDirectory)
                |> Seq.map (fun x -> x.Prefix)
        let dirCount = Seq.length dirs

        printfn "%d event blob directories" dirCount

        let getBlobStats (blobs : seq<Blob.CloudBlockBlob>) =
            let count = Seq.length blobs
            match count with
            | 0 -> (0, 0L)
            | _ -> (count, (int64)count * (Seq.head blobs).Properties.Length)

        let isOfDate (blobName : string) year month day =
            let rowKey = Seq.last (blobName.Split('/'))
            let time = Utils.rowKeyToTime rowKey
            let dateFrom = DateTime(year, month, day)
            let dateTo = dateFrom.AddDays(1.)
            time >= dateFrom && time < dateTo

        let (count, size) = dirs 
                            |> Seq.map (fun x -> eventBlobContainer.ListBlobs(x)
                                                |> Seq.map (fun x -> x :?> Blob.CloudBlockBlob) 
                                                |> Seq.filter (fun x -> isOfDate x.Name 2015 1 15)
                                                |> getBlobStats)
                            |> Seq.fold (fun (count, size) (x, y) -> (count + x, size + y)) (0, 0L)

        printfn "%d blobs of %i KB in %s" count (sizeInKilobytes (size / (int64)count)) containerName |> ignore

    [<TestCase("getsituation")>]
    let ``should load situation events`` (containerName) =

        let (_, _, eventBlobContainer) = getStorageContainers containerName false

        let dirs = eventBlobContainer.ListBlobs("")
                |> Seq.filter (fun x -> x :? Blob.CloudBlobDirectory)
                |> Seq.map (fun x -> x :?> Blob.CloudBlobDirectory)
                |> Seq.map (fun x -> x.Prefix)
        let dirCount = Seq.length dirs

        let saveBlob (blob : Blob.CloudBlockBlob) (i : int) =
            let text = blob.DownloadText()
            use writer = System.IO.StreamWriter((i+1).ToString() + ".xml")
            writer.Write(text)
            ()

        let dir = dirs |> Seq.skip 40 |> Seq.head
        let blobs = eventBlobContainer.ListBlobs(dir) |> Seq.map (fun x -> x :?> Blob.CloudBlockBlob) 
        printfn "Loading %d events from %s" (Seq.length blobs) dir
        blobs |> Seq.iteri (fun i x -> saveBlob x i)