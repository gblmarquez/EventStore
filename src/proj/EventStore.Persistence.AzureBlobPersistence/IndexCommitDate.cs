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
    using Lucene.Net.Store.Azure;
    using Microsoft.WindowsAzure;
    using Lucene.Net.Store;

    public class IndexCommitDate : IDisposable
    {
        private readonly object sync = new object();
        private Microsoft.WindowsAzure.CloudStorageAccount storageAccount;
        private string containerAddress;
        private string indexCatalog;
        private AzureDirectory directory;
        private IndexWriter index;
        private bool disposed;
        private RAMDirectory cacheDirectory;

        public IndexCommitDate(CloudStorageAccount storageAccount, string containerAddress)
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
            this.indexCatalog = string.Concat(this.containerAddress, "-index-commit-date");
            this.cacheDirectory = new RAMDirectory();
            this.directory = new AzureDirectory(this.storageAccount, this.indexCatalog, this.cacheDirectory);
        }

        public void Add(Commit commit)
        {
            var doc = new Document();

            doc.Add(new NumericField("date", 1, Field.Store.YES, true).SetLongValue(commit.CommitStamp.Ticks));
            doc.Add(new Field("uri", commit.ToCommitAddress(), Field.Store.COMPRESS, Field.Index.NO));

            lock (sync)
            {
                using (var index = new IndexWriter(this.directory, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_29), !IndexReader.IndexExists(this.directory), IndexWriter.MaxFieldLength.LIMITED))
                {
                    index.AddDocument(doc);
                    index.Commit();
                    index.Close();
                }
            }
        }

        public IEnumerable<string> From(DateTime value)
        {
            if (IndexReader.IndexExists(this.directory) == false)
            {
                yield return string.Empty;
            }

            var searcher = new IndexSearcher(this.directory, true);
            var query = NumericRangeQuery.NewLongRange(
                                                "date",
                                                value.Ticks,
                                                DateTime.MaxValue.Ticks,
                                                true,
                                                true);

            var hits = searcher.Search(query);
            if (hits.Length() > 0)
            {
                var it = hits.Iterator();
                it.Reset();
                while (it.MoveNext())
                {
                    yield return ((Hit)it.Current).GetDocument().GetField("uri").StringValue();
                }
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
