namespace EventStore.Persistence.AzureBlobPersistence
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Globalization;

    public static class ExtensionMethods
    {
        public static string ToCommitAddress(this Commit commit)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}/{1}", commit.StreamId, commit.ToCommitId());
        }

        public static string ToUndispatchedCommitAddress(this Commit commit)
        {
            return string.Format(CultureInfo.InvariantCulture, "undispatched-commits/{0}", commit.ToCommitAddress());
        }

        public static string ToCommitId(this Commit commit)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}.commit", commit.CommitSequence);
        }

        public static string ToStreamId(this Commit commit)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}", commit.StreamId);
        }

        public static string ToStreamHeadAddress(this StreamHead head)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}/head", head.StreamId);
        }

        public static string ToStreamHeadAddress(this Guid id)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}/head", id);
        }

        public static StreamHead ToStreamHead(this AzureBlobStreamHead head)
        {
            return new StreamHead(head.StreamId, head.HeadRevision, head.SnapshotRevision);
        }

        public static AzureBlobStreamHead ToAzureBlobStreamHead(this StreamHead head)
        {
            return new AzureBlobStreamHead(head.StreamId, head.HeadRevision, head.SnapshotRevision);
        }
    }
}
