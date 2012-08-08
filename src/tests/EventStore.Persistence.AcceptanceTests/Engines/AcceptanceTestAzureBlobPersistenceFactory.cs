namespace EventStore.Persistence.AcceptanceTests.Engines
{
    using EventStore.Persistence.AzureBlobPersistence;
    using EventStore.Serialization;

    public class AcceptanceTestAzureBlobPersistenceFactory : AzureBlobPersistenceFactory
   {
        private static readonly AzureBlobConfiguration Config = new AzureBlobConfiguration
        {
            Serializer = new BinarySerializer(),
            ConnectionName = "AzureBlob",
            ContainerAddress = "event-store-acceptance-test"
        };

        public AcceptanceTestAzureBlobPersistenceFactory()
            : base(Config)
        {
        }
    }
}
