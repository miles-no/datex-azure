namespace Vegvesen.EventExtractor

open System
open System.IO
open Microsoft.WindowsAzure.Storage

open BlobStorage
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

    let rec extractServiceEvents (blobContainer : Blob.CloudBlobContainer) (lastBlobName : string option) (unprocessedBlobNames : string list) =
        
        match unprocessedBlobNames with
        | [] -> ()
        | nextBlobName :: restBlobNames ->
            let nextBlobNodes = extractXmlNodes blobContainer nextBlobName
            let diffBlobNodes = match lastBlobName with
            | None ->
                nextBlobNodes
            | Some(lastBlobName) ->
                let lastBlobNodes = extractXmlNodes blobContainer lastBlobName
                extractDiff lastBlobNodes nextBlobNodes            
            printfn "Total: %i, diff: %i" (nextBlobNodes |> Seq.length) (diffBlobNodes |> Seq.length)
            extractServiceEvents blobContainer (Some(nextBlobName)) restBlobNames

    let updateServiceEvents (account : CloudStorageAccount) containerName =
        
        let blobClient = account.CreateCloudBlobClient()
        let blobContainer = blobClient.GetContainerReference(containerName)
        let lastExtractedBlobName = getLastExtractedBlobName blobContainer
        let lastBlobName = 
            match lastExtractedBlobName with
            | Some(lastExtractedBlobName) -> 
                lastExtractedBlobName
            | None -> "2014/12/31/000000"

        let unprocessedBlobs = enumerateBlobs blobContainer lastBlobName |> Seq.take 100 |> List.ofSeq
        extractServiceEvents blobContainer lastExtractedBlobName unprocessedBlobs
