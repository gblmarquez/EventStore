namespace EventStore.Persistence.AzureBlobPersistence
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Lucene.Net.Analysis.Standard;
    using Lucene.Net.Documents;
    using Lucene.Net.Index;
    using Lucene.Net.Search;
    using Lucene.Net.Store;
    using Lucene.Net.Store.Azure;
    using Microsoft.WindowsAzure;

    public class IndexStreamsToSnapshot : IDisposable
    {
        private readonly object sync = new object();
        private Microsoft.WindowsAzure.CloudStorageAccount storageAccount;
        private string containerAddress;
        private string indexCatalog;
        private AzureDirectory directory;
        private bool disposed;
        private RAMDirectory cacheDirectory;

        public IndexStreamsToSnapshot(CloudStorageAccount storageAccount, string containerAddress)
        {
            this.storageAccount = storageAccount;
            this.containerAddress = containerAddress;
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

            //Logger.Debug(Messages.ShuttingDownPersistence);
            this.disposed = true;

            if (this.cacheDirectory != null)
            {
                this.cacheDirectory.Close();
                this.cacheDirectory.Dispose();
            }

            if (this.directory != null)
            {
                this.directory.Close();
                this.directory.Dispose();
            }
        }

        public void Initialize()
        {
            this.indexCatalog = string.Concat(this.containerAddress, "-idx-stream-head-age");
            this.cacheDirectory = new RAMDirectory();
            this.directory = new AzureDirectory(this.storageAccount, this.indexCatalog, this.cacheDirectory);
        }

        public void Add(StreamHead head)
        {
            var id = head.StreamId.ToString();

            var doc = new Document();
            doc.Add(new NumericField("age", 1, Field.Store.YES, true).SetLongValue(head.HeadRevision - head.SnapshotRevision));
            doc.Add(new Field("id", id, Field.Store.COMPRESS, Field.Index.NO));

            var indexExists = IndexReader.IndexExists(this.directory);
            if (indexExists)
            {
                using (var reader = IndexReader.Open(this.directory, true))
                {
                    reader.DeleteDocuments(new Term("id", id));
                }
            }
            using (var writer = new IndexWriter(this.directory, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_29), !indexExists, IndexWriter.MaxFieldLength.LIMITED))
            {
                writer.AddDocument(doc);
                writer.Commit();
                writer.Close();
            }
        }

        public IEnumerable<Guid> From(int maxThreshold)
        {
            if (IndexReader.IndexExists(this.directory) == false)
            {
                return Enumerable.Empty<Guid>();
            }

            var query = NumericRangeQuery.NewIntRange("age", maxThreshold, int.MaxValue, true, true);
            using (var searcher = new IndexSearcher(this.directory, true))
            {
                var hits = searcher.Search(query, null, int.MaxValue);
                if (hits.TotalHits > 0)
                {
                    return hits.ScoreDocs
                               .Select(e => new Guid(searcher.Doc(e.doc).Get("id")));
                }
            }
            return Enumerable.Empty<Guid>();
        }

        public void Purge()
        {
            this.directory.ClearCache();

            using (var writer = new IndexWriter(this.directory, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_29), false, IndexWriter.MaxFieldLength.LIMITED))
            {
                writer.DeleteAll();
                writer.Commit();
                writer.Close();
            }
        }
    }
}
