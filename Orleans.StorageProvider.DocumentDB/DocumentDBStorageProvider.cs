using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Orleans.Runtime;
using Orleans.Storage;
using System;
using System.Threading.Tasks;

namespace Orleans.StorageProvider.DocumentDB
{

    // TODO: Ensure collection exists
    // TODO: CI
    // TODO: Testing
    // TODO: Nuget
    // TODO: readme
    // TODO: extension method for registering the provider


    public class DocumentDBStorageProvider : IStorageProvider
    {
        private string databaseName;
        private string collectionName;
        private int throughput;
        public string Name { get; set; }
        public Logger Log { get; set; }

        private DocumentClient Client { get; set; }

        public async Task Init(string name, Providers.IProviderRuntime providerRuntime, Providers.IProviderConfiguration config)
        {
            try
            {
                Name = name;
                var url = config.Properties["Url"];
                var key = config.Properties["Key"];
                databaseName = config.Properties["Database"];
                collectionName = config.Properties["Collection"];
                throughput = int.Parse(config.Properties["Throughput"]);
                
                Client = new DocumentClient(new Uri(url), key);

                await Client.CreateDatabaseIfNotExistsAsync(new Database { Id = this.databaseName });
                
                var myCollection = new DocumentCollection
                {
                    Id = collectionName
                };
                await Client.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(databaseName),
                    myCollection,
                    new RequestOptions { OfferThroughput = throughput});
                
            }
            catch (Exception ex)
            {
                Log.Error(0, "Error in Init.", ex);
                throw;
            }
        }

        public Task Close()
        {
            if (null != Client) Client.Dispose();

            return Task.CompletedTask;
        }

        string GetDocumentId(string grainType, GrainReference reference)
        {
            var id = reference.GetPrimaryKey(out string extension);
           
            return $"{grainType}-{id}{extension}"; 
        }

        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            try
            {
                var uri = UriFactory.CreateDocumentUri(databaseName,collectionName,
                    GetDocumentId(grainState.State.GetType().Name, grainReference));
                Document readDoc = await Client.ReadDocumentAsync(uri);

                if (null != readDoc)
                {
                    grainState.ETag = readDoc.ETag;
                    grainState.State = JsonConvert.DeserializeObject(readDoc.ToString(), 
                        grainState.State.GetType());
                }
            }
            catch(DocumentClientException dc) when 
            (dc.StatusCode == System.Net.HttpStatusCode.NotFound){ /*not found is ok*/  }
            catch (Exception ex)
            {
                Log.Error(0, "Error in ReadStateAsync", ex);
                throw;
            }            
        }

        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            try
            {
                var uri = UriFactory.CreateDocumentUri(databaseName, collectionName,
                    GetDocumentId(grainState.State.GetType().Name, grainReference));
                var document = grainState.State;
                
                if (null != grainState.ETag)
                {
                    var ac = new AccessCondition {
                        Condition = grainState.ETag,
                        Type = AccessConditionType.IfMatch };
                    await Client.ReplaceDocumentAsync(uri, document, new RequestOptions { AccessCondition = ac });
                }
                else
                {
                    Document newDoc = await Client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), document);
                    grainState.ETag = newDoc.ETag;
                }
            }
            catch (Exception ex)
            {
                Log.Error(0, "Error in WriteStateAsync", ex);
                throw;
            }
        }

        public async Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            try
            {
                var uri = UriFactory.CreateDocumentUri(databaseName,collectionName,
                    GetDocumentId(grainState.State.GetType().Name, grainReference));
                await Client.DeleteDocumentAsync(uri);
                grainState.State = null;
                grainState.ETag = null;
            }
            catch (Exception ex)
            {
                Log.Error(0, "Error in ClearStateAsync", ex);
                throw;
            }
        }

      

    }
}