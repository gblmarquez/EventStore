namespace EventStore.Persistence.AzureBlobPersistence
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Lucene.Net.Analysis.Standard;
    using Lucene.Net.Documents;
    using Lucene.Net.Index;
    using Lucene.Net.QueryParsers;
    using Lucene.Net.Search;
    using Lucene.Net.Store;
    using Lucene.Net.Store.Azure;
    using Microsoft.WindowsAzure;

    public class IndexCommitStreamRevision
    {
        private static readonly Lucene.Net.Util.Version LUCENE_VERSION = Lucene.Net.Util.Version.LUCENE_29;
        private readonly object sync = new object();
        private Microsoft.WindowsAzure.CloudStorageAccount storageAccount;
        private string containerAddress;
        private string indexCatalog;
        private AzureDirectory directory;
        private RAMDirectory cacheDirectory;
        private bool disposed;

        public IndexCommitStreamRevision(CloudStorageAccount storageAccount, string containerAddress)
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
            this.indexCatalog = string.Concat(this.containerAddress, "-index-commit-stream-revision");
            this.cacheDirectory = new RAMDirectory();
            this.directory = new AzureDirectory(this.storageAccount, this.indexCatalog, this.cacheDirectory);
        }

        public void Add(Commit commit)
        {
            var doc = new Document();
            doc.Add(new Field("streamId", commit.StreamId.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
            doc.Add(new NumericField("streamRevision", 1, Field.Store.YES, true).SetIntValue(commit.StreamRevision));
            doc.Add(new NumericField("startStreamRevision", 1, Field.Store.YES, true).SetIntValue(commit.StreamRevision - (commit.Events.Count - 1)));
            doc.Add(new Field("uri", commit.ToCommitAddress(), Field.Store.COMPRESS, Field.Index.NO));

            lock (sync)
            {
                using (var index = new IndexWriter(this.directory, new StandardAnalyzer(LUCENE_VERSION), !IndexReader.IndexExists(this.directory), IndexWriter.MaxFieldLength.LIMITED))
                {
                    index.AddDocument(doc);
                    index.Commit();
                    index.Close();
                }
            }
        }

        public IEnumerable<string> From(Guid streamId, int minRevision, int maxRevision)
        {
            if (IndexReader.IndexExists(this.directory) == false)
            {
                return Enumerable.Empty<string>();
            }

            var minQ = NumericRangeQuery.NewIntRange("streamRevision", minRevision, int.MaxValue, true, true);
            var maxQ = NumericRangeQuery.NewIntRange("startStreamRevision", int.MinValue, maxRevision, true, true);

            BooleanQuery bq = new BooleanQuery();
            TermQuery idQ = new TermQuery(new Term("streamId", streamId.ToString()));
            bq.Add(idQ, BooleanClause.Occur.MUST);
            bq.Add(minQ, BooleanClause.Occur.MUST);
            bq.Add(maxQ, BooleanClause.Occur.MUST);

            var searcher = new IndexSearcher(this.directory, true);
            var hits = searcher.Search(bq, null, int.MaxValue);
            if (hits.TotalHits > 0)
            {
                return hits.ScoreDocs
                           .Select(e => searcher.Doc(e.doc).Get("uri"));
            }
            else
            {
                return Enumerable.Empty<string>();
            }
        }

        public void Purge()
        {
            this.directory.ClearCache();

            using (var index = new IndexWriter(this.directory, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_29), !IndexReader.IndexExists(this.directory), IndexWriter.MaxFieldLength.LIMITED))
            {
                index.DeleteAll();
                index.Commit();
                index.Close();
            }
        }
    }
}
