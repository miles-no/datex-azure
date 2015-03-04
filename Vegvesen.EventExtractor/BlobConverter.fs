namespace Vegvesen.EventExtractor

open System
open System.IO
open Microsoft.FSharp.Collections
open Microsoft.WindowsAzure.Storage

[<AutoOpen>]
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

    let updateLastExtractedBlobName (blobContainer : Blob.CloudBlobContainer) (blobName : string option) =
        let blob = blobContainer.GetBlockBlobReference("LastExtracted")
        match blobName with
        | Some blobName -> blob.UploadText(blobName)
        | None -> blob.DeleteIfExists() |> ignore

    let extractXmlNodes (blobContainer : Blob.CloudBlobContainer) blobName =
        let (elementName, idElementName) = getContainerXmlElementNames blobContainer.Name
        let blob = blobContainer.GetBlockBlobReference(blobName)
        use stream = blob.OpenRead()
        parseNodes stream elementName idElementName

    let extractServiceEvents (blobContainer : Blob.CloudBlobContainer) lastBlobName nextBlobName =
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

    let updateServiceEvents (account : CloudStorageAccount) containerName maxBlobCount =        
        let blobClient = account.CreateCloudBlobClient()
        let rawBlobContainer = blobClient.GetContainerReference(containerName)
        let tableClient = account.CreateCloudTableClient()
        let table = tableClient.GetTableReference(containerName)
        table.CreateIfNotExists() |> ignore
        let eventBlobContainer = blobClient.GetContainerReference(containerName + "-events")
        eventBlobContainer.CreateIfNotExists() |> ignore

        let lastExtractedBlobName = getLastExtractedBlobName rawBlobContainer
        let (startBlobName, lastBlobName) = 
            match lastExtractedBlobName with
            | Some(lastExtractedBlobName) -> (lastExtractedBlobName, lastExtractedBlobName)
            | None -> ("2014/12/31/000000", "")

        let unprocessedBlobs = enumerateBlobs rawBlobContainer startBlobName |> Seq.truncate maxBlobCount |> List.ofSeq
        match List.isEmpty unprocessedBlobs with
        | true -> ()
        | false ->
            let blobPairs = (lastBlobName, List.head unprocessedBlobs) :: (getPairwise unprocessedBlobs)
            blobPairs 
            |> PSeq.map (fun (last, next) -> extractServiceEvents rawBlobContainer last next)
            |> PSeq.iter (fun (publicationTime, events) -> 
                        (events |> PSeq.iter (fun event -> saveEvent table eventBlobContainer event publicationTime)))

            let lastBlobName = unprocessedBlobs |> Seq.last
            printfn "Last processed blob: %s" lastBlobName
            updateLastExtractedBlobName rawBlobContainer (Some lastBlobName)
