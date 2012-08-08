namespace EventStore.Persistence.AzureBlobPersistence
{
    using System;
    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.StorageClient;

    public class AzureBlobPersistenceFactory : IPersistenceFactory
    {
        private readonly AzureBlobConfiguration config;

        public AzureBlobPersistenceFactory(AzureBlobConfiguration config)
        {
            if (config == null)
                throw new ArgumentNullException("config");

            this.config = config;
        }

        public virtual IPersistStreams Build()
        {
            // Retrieve storage account from connection-string
            var storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting(this.config.ConnectionName));

            return new AzureBlobPersistenceEngine(storageAccount, this.config.ContainerAddress, this.config.Serializer);
        }
    }
}
