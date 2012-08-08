namespace EventStore.Persistence.AzureBlobPersistence
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using EventStore.Serialization;

    public class AzureBlobConfiguration
    {
        public string ConnectionName { get; set; }
        public string ContainerAddress { get; set; }
        public ISerialize Serializer { get; set; }
    }
}
