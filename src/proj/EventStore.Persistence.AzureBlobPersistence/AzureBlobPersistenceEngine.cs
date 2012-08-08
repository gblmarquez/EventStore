namespace EventStore.Persistence.AzureBlobPersistence
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using EventStore.Logging;
    using EventStore.Serialization;
    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.StorageClient;

    public class AzureBlobPersistenceEngine : IPersistStreams
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof(AzureBlobPersistenceEngine));

        private readonly CloudBlobClient storageClient;
        private readonly string containerAddress;
        private readonly ISerialize serializer;
        private CloudBlobDirectory container;

        private int initialized;
        private bool disposed;
        private IndexCommitDate indexDate;
        private IndexCommitStreamRevision indexStreamRevision;
        private IndexStreamsToSnapshot indexStreamAge;

        public AzureBlobPersistenceEngine(CloudStorageAccount storageAccount, string containerAddress, ISerialize serializer)
        {
            if (storageAccount == null)
                throw new ArgumentNullException("storageAccount");

            if (string.IsNullOrWhiteSpace(containerAddress))
                throw new ArgumentNullException("containerAddress");

            if (serializer == null)
                throw new ArgumentNullException("serializer");

            this.storageClient = storageAccount.CreateCloudBlobClient();
            this.containerAddress = containerAddress;
            this.serializer = serializer;

            this.indexDate = new IndexCommitDate(storageAccount, containerAddress);
            this.indexStreamRevision = new IndexCommitStreamRevision(storageAccount, containerAddress);
            this.indexStreamAge = new IndexStreamsToSnapshot(storageAccount, containerAddress);
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected virtual void Dispose(bool disposing)
        {
            if (!disposing || this.disposed)
                return;

            Logger.Debug(Messages.ShuttingDownPersistence);
            this.disposed = true;

            this.indexDate.Dispose();
            this.indexStreamRevision.Dispose();
            this.indexStreamAge.Dispose();
        }

        public void Initialize()
        {
            if (Interlocked.Increment(ref this.initialized) > 1)
                return;

            Logger.Debug(Messages.InitializingStorage);

            // Retrieve a reference to a container 
            this.container = this.storageClient.GetBlobDirectoryReference(this.containerAddress);

            // Create the container if it doesn't already exist
            this.container.Container.CreateIfNotExist();

            // Initiliazes indexes
            this.indexDate.Initialize();
            this.indexStreamRevision.Initialize();
            this.indexStreamAge.Initialize();
        }

        public IEnumerable<Commit> GetUndispatchedCommits()
        {
            var blobs = this.container.GetSubdirectory("undispatched-commits")
                                      .ListBlobs(new BlobRequestOptions()
                                      {
                                          UseFlatBlobListing = true
                                      });

            return blobs.Select(e => this.serializer.Deserialize<Commit>(
                                        this.container.GetBlobReference(e.Uri.ToString().Replace("undispatched-commits/", string.Empty))
                                                        .DownloadByteArray()));
        }

        public void MarkCommitAsDispatched(Commit commit)
        {
            this.container.GetBlobReference(commit.ToUndispatchedCommitAddress())
                          .DeleteIfExists();
        }

        private void SetUndispatchedCommit(Commit commit)
        {
            var blob = this.container.GetBlobReference(commit.ToUndispatchedCommitAddress());
            string etag = blob.Properties.ETag;

            var bro = etag != null
                ? new BlobRequestOptions { AccessCondition = AccessCondition.IfMatch(etag) }
                : new BlobRequestOptions { AccessCondition = AccessCondition.IfNoneMatch("*") };

            blob.Properties.ContentMD5 = null;
            blob.UploadText(string.Empty);
        }

        public void Purge()
        {
            Logger.Warn(Messages.PurgingStorage);

            this.container.ListBlobs(new BlobRequestOptions()
                                    {
                                        UseFlatBlobListing = true
                                    })
                          .AsParallel()
                          .ForAll(e => this.container.GetBlobReference(e.Uri.ToString()).DeleteIfExists());

            // Purge indexes
            this.indexDate.Purge();
            this.indexStreamRevision.Purge();
            this.indexStreamAge.Purge();
        }

        public IEnumerable<Commit> GetFrom(DateTime start)
        {
            return this.indexDate
                       .From(start)
                       .Where(e => e != string.Empty)
                       .Select(e =>
                                this.serializer
                                    .Deserialize<Commit>(this.container.GetBlobReference(e).DownloadByteArray()));
        }

        public IEnumerable<Commit> GetFrom(Guid streamId, int minRevision, int maxRevision)
        {
            return this.indexStreamRevision
                       .From(streamId, minRevision, maxRevision)
                       .Where(e => e != string.Empty)
                       .Select(e => new { Uri = e, Revision = int.Parse(e.Remove(0, e.LastIndexOf('/') + 1).Replace(".commit", string.Empty)) })
                       .Select(e =>
                                this.serializer
                                    .Deserialize<Commit>(this.container.GetBlobReference(e.Uri).DownloadByteArray()));
        }

        public void Commit(Commit attempt)
        {
            CloudBlob blob;

            if (TryGetBlobReference(attempt, out blob) == false)
            {
                using (var memory = new MemoryStream())
                {
                    this.serializer.Serialize(memory, attempt);
                    try
                    {
                        string etag = blob.Properties.ETag;
                        var bro = etag != null
                                    ? new BlobRequestOptions { AccessCondition = AccessCondition.IfMatch(etag) }
                                    : new BlobRequestOptions { AccessCondition = AccessCondition.IfNoneMatch("*") };

                        blob.Properties.ContentMD5 = null;
                        blob.UploadByteArray(memory.ToArray(), bro);
                        this.UpdateStreamHeadAsync(attempt.StreamId, attempt.StreamRevision, attempt.Events.Count);
                    }
                    catch (Exception)
                    {
                        Logger.Debug(Messages.ConcurrentWriteDetected);
                        throw new ConcurrencyException();
                    }
                }
                this.SetUndispatchedCommit(attempt);
                this.IndexCommit(attempt);
            }
            else
            {
                throw new DuplicateCommitException();
            }
        }

        private void IndexCommit(EventStore.Commit commit)
        {
            this.indexDate.Add(commit);
            this.indexStreamRevision.Add(commit);
        }

        public Snapshot GetSnapshot(Guid streamId, int maxRevision)
        {
            return default(Snapshot);
        }

        public bool AddSnapshot(Snapshot snapshot)
        {
            return false;
        }

        public IEnumerable<StreamHead> GetStreamsToSnapshot(int maxThreshold)
        {
            var ret = this.indexStreamAge
                          .From(maxThreshold)
                          .Select(e => 
                              this.serializer
                                  .Deserialize<AzureBlobStreamHead>(this.container.GetBlobReference(e.ToStreamHeadAddress()).DownloadByteArray())
                                  .ToStreamHead());
            
            return ret;
        }

        private void UpdateStreamHeadAsync(Guid streamId, int streamRevision, int eventsCount)
        {
            ThreadPool.QueueUserWorkItem(x =>
            {
                CloudBlob blob;
                var head = new StreamHead(streamId, streamRevision, 0);
                if (TryGetBlobReference(head, out blob) == false)
                {
                    using (var memory = new MemoryStream())
                    {
                        this.serializer.Serialize(memory, head.ToAzureBlobStreamHead());
                        try
                        {
                            blob.Properties.ContentMD5 = null;
                            blob.UploadByteArray(memory.ToArray(), new BlobRequestOptions { AccessCondition = AccessCondition.IfNoneMatch("*") });
                            this.IndexHead(head);
                        }
                        catch (Exception)
                        {
                        }
                    }
                }
                head = null;
            }, null);
        }

        private void IndexHead(StreamHead head)
        {
            this.indexStreamAge.Add(head);
        }

        protected virtual bool TryGetBlobReference(StreamHead head, out CloudBlob blob)
        {
            blob = this.container.GetBlobReference(head.ToStreamHeadAddress());
            try
            {
                blob.FetchAttributes();
                return true;
            }
            catch (StorageClientException e)
            {
                if (e.ErrorCode == StorageErrorCode.ResourceNotFound)
                {
                    return false;
                }
                else
                {
                    throw;
                }
            }
        }

        protected virtual bool TryGetBlobReference(Commit commit, out CloudBlob blob)
        {
            if (disposed)
                throw new ObjectDisposedException("AzureBlobPersistenceEngine");

            blob = null;
            if (commit == null || this.container == null)
                return false;

            blob = this.container.GetBlobReference(commit.ToCommitAddress());
            try
            {
                blob.FetchAttributes();
                return true;
            }
            catch (StorageClientException e)
            {
                if (e.ErrorCode == StorageErrorCode.ResourceNotFound)
                {
                    return false;
                }
                else
                {
                    throw;
                }
            }
        }

        protected virtual CloudBlobDirectory GetStreamContainer(Guid streamId)
        {
            var streamContainer = this.container.GetSubdirectory(streamId.ToString());
            return streamContainer;
        }
    }
}
