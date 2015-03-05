namespace Vegvesen.EventExtractor.WebJob

open System
open System.Configuration
open System.Collections.Generic
open System.Threading.Tasks
open Microsoft.WindowsAzure.Storage
open Microsoft.Azure.WebJobs
open Vegvesen.EventExtractor

module WebJob =

    let getStorageAccounts =
        let sourceAccount = CloudStorageAccount.Parse(ConfigurationManager.ConnectionStrings.Item("AzureWebJobsStorage").ConnectionString)
        let eventAccount = CloudStorageAccount.Parse(ConfigurationManager.ConnectionStrings.Item("VegvesenEventStorage").ConnectionString)
        (sourceAccount, eventAccount)

    [<EntryPoint>]
    let main argv = 

        let (sourceAccount, eventAccount) = getStorageAccounts

        let tasks = List<Task>()

        for containerName in blobContainers do
            try
                let containerName = containerName.ToLower()
                match containerName with
                | "getcctvsitetable" -> () // skip CCTV events, nothing interesting
                | _ -> updateServiceEvents sourceAccount eventAccount containerName 10
            with
            | ex -> printfn "Error while processing events for %s: %s" containerName ex.Message
        0
