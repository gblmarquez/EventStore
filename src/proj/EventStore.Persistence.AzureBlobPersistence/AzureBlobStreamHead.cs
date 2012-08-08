namespace EventStore.Persistence.AzureBlobPersistence
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    [Serializable]
    public class AzureBlobStreamHead
    {
        public AzureBlobStreamHead(Guid streamId, int headRevision, int snapshotRevision)
        {
            this.StreamId = streamId;
            this.HeadRevision = headRevision;
            this.SnapshotRevision = snapshotRevision;
        }

        public Guid StreamId { get; set; }
        public int HeadRevision { get; set; }
        public int SnapshotRevision { get; set; }
    }
}
