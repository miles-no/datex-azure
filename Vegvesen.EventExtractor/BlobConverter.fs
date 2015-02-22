namespace Vegvesen.EventExtractor

open System
open System.IO
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage

open BlobStorage
open TableStorage
open XmlParser

module BlobConverter =

    let blobContainers = [|
        "GetMeasurementWeatherSiteTable"
        "GetMeasuredWeatherData"
        "GetCCTVSiteTable"
        "GetPredefinedTravelTimeLocations"
        "GetTravelTimeData"
        "GetSituation"
    |]

    let getContainerXmlElementNames (containerName : string) =
        match containerName.ToLower() with
        | "getmeasurementweathersitetable" -> ("measurementSiteRecord", "measurementSiteRecord")
        | "getmeasuredweatherdata" -> ("siteMeasurements", "measurementSiteReference")
        | "getcctvsitetable" -> ("cctvCameraMetadataRecord", "cctvCameraMetadataRecord")
        | "getpredefinedtraveltimelocations" -> ("predefinedLocationContainer", "predefinedLocationContainer")
        | "gettraveltimedata" -> ("elaboratedData", "predefinedLocationReference")
        | "getsituation" -> ("situation", "situation")
        | _ -> failwith <| sprintf "Unknown container name \"%s\"" containerName

    let getLastExtractedBlobName (blobContainer : Blob.CloudBlobContainer) =
        let blob = blobContainer.GetBlockBlobReference("LastExtracted")
        if blob.Exists() then
            Some(blob.DownloadText())
        else
            None

    let updateLastExtractedBlobName (blobContainer : Blob.CloudBlobContainer) blobName =
        let blob = blobContainer.GetBlockBlobReference("LastExtracted")
        blob.UploadText(blobName)

    let extractXmlNodes (blobContainer : Blob.CloudBlobContainer) blobName =
        let (elementName, idElementName) = getContainerXmlElementNames blobContainer.Name
        let blob = blobContainer.GetBlockBlobReference(blobName)
        use stream = blob.OpenRead()
        parseNodes stream elementName idElementName

    let extractServiceEvents (blobContainer : Blob.CloudBlobContainer) lastBlobName nextBlobName =
        printfn "extractServiceEvents: %s" nextBlobName
        let (publicationTime, nextBlobNodes) = extractXmlNodes blobContainer nextBlobName
        match lastBlobName with
        | "" ->
            (publicationTime, nextBlobNodes)
        | _ ->
            let (_, lastBlobNodes) = extractXmlNodes blobContainer lastBlobName
            (publicationTime, extractDiff lastBlobNodes nextBlobNodes)

    let rec getPairwise lst =
        match lst with
        | [] -> []
        | [item] -> []
        | first :: second :: tail -> (first, second) :: getPairwise (second :: tail)

    let updateServiceEvents (account : CloudStorageAccount) containerName =        
        let blobClient = account.CreateCloudBlobClient()
        let blobContainer = blobClient.GetContainerReference(containerName)
        let tableClient = account.CreateCloudTableClient()

        let lastExtractedBlobName = getLastExtractedBlobName blobContainer
        let lastBlobName = 
            match lastExtractedBlobName with
            | Some(lastExtractedBlobName) -> 
                lastExtractedBlobName
            | None -> "2014/12/31/000000"

        let unprocessedBlobs = enumerateBlobs blobContainer lastBlobName |> Seq.take 5 |> List.ofSeq
        let blobPairs = ("", List.head unprocessedBlobs) :: (getPairwise unprocessedBlobs)
        blobPairs 
        |> PSeq.map (fun (x, y) -> 
                    let (publicationTime, events) = extractServiceEvents blobContainer x y
                    events |> PSeq.iter (fun x -> saveEvent tableClient containerName x publicationTime)) 
        |> List.ofSeq
