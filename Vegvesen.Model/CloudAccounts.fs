namespace Vegvesen.Model

open System
open System.Configuration
open Microsoft.WindowsAzure.Storage
open Microsoft.Azure.Documents.Client
open Elasticsearch.Net
open Elasticsearch.Net.Connection

[<AutoOpen>]
module CloudAccounts =

    type ConnectionString(name : string) = 
        member this.Value = ConfigurationManager.ConnectionStrings.Item(name).ConnectionString

    type AccountInfo() =

        let sourceConnectionString = ConnectionString("AzureWebJobsStorage").Value
        let eventConnectionString = ConnectionString("VegvesenEventStorage").Value
        let documentConnectionString = ConnectionString("VegvesenEventDocumentDB").Value
        let elasticConnectionString = ConnectionString("VegvesenEventElasticsearch").Value

        let sourceAccount = CloudStorageAccount.Parse sourceConnectionString
        let sourceBlobClient = sourceAccount.CreateCloudBlobClient()
        let eventAccount = CloudStorageAccount.Parse eventConnectionString
        let eventTableClient = eventAccount.CreateCloudTableClient()
        let eventBlobClient = eventAccount.CreateCloudBlobClient()

        let documentClient (connectionString : string) = 
            let endpointKV = connectionString.Split(';') |> Seq.head
            let authorizaionKV = connectionString.Split(';') |> Seq.last
            let documentUri = endpointKV.Substring(string("EndpointUrl=").Length)
            let documentPassword = authorizaionKV.Substring(string("AuthorizationKey=").Length)
            new DocumentClient(Uri(documentUri), documentPassword)

        let elasticClient (connectionString : string) =
            let serverUri = connectionString.Substring(string("EndpointUrl=").Length)
            let config = ConnectionConfiguration(Uri(serverUri))
            new ElasticsearchClient(config)

        member this.SourceXmlBlobClient = sourceBlobClient
        member this.EventXmlTableClient = eventTableClient
        member this.EventXmlBlobClient = eventBlobClient
        member this.EventJsonBlobClient = eventBlobClient
        member this.CoordinateJsonBlobClient = eventBlobClient
        member this.EventDocumentClient = documentClient documentConnectionString
        member this.EventElasticClient = elasticClient elasticConnectionString
