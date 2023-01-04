
using System.Diagnostics;


namespace EXBP.Dipren.Data.Tests
{
    [DebuggerStepThrough]
    internal class EngineDataStoreWrapper : IEngineDataStore, IDisposable, IAsyncDisposable
    {
        private readonly IEngineDataStore _store;


        public EngineDataStoreWrapper(IEngineDataStore store)
        {
            Debug.Assert(store != null);

            this._store = store;
        }


        public void Dispose()
        {
            IDisposable disposable = (this._store as IDisposable);

            if (disposable != null)
            {
                disposable.Dispose();

                GC.SuppressFinalize(this);
            }
        }

        public async ValueTask DisposeAsync()
        {
            IAsyncDisposable disposable = (this._store as IAsyncDisposable);

            if (disposable != null)
            {
                await disposable.DisposeAsync();

                GC.SuppressFinalize(this);
            }
        }


        public Task<long> CountIncompletePartitionsAsync(string id, CancellationToken cancellation)
            => this._store.CountIncompletePartitionsAsync(id, cancellation);

        public Task<long> CountJobsAsync(CancellationToken cancellation)
            => this._store.CountJobsAsync(cancellation);

        public Task InsertJobAsync(Job job, CancellationToken cancellation)
            => this._store.InsertJobAsync(job, cancellation);

        public Task InsertPartitionAsync(Partition partition, CancellationToken cancellation)
            => this._store.InsertPartitionAsync(partition, cancellation);

        public Task InsertSplitPartitionAsync(Partition partitionToUpdate, Partition partitionToInsert, CancellationToken cancellation)
            => this._store.InsertSplitPartitionAsync(partitionToUpdate, partitionToInsert, cancellation);

        public Task<Partition> ReportProgressAsync(Guid id, string owner, DateTime timestamp, string position, long processed, long remaining, bool completed, double throughput, CancellationToken cancellation)
            => this._store.ReportProgressAsync(id, owner, timestamp, position, processed, remaining, completed, throughput, cancellation);

        public Task<Job> RetrieveJobAsync(string id, CancellationToken cancellation)
            => this._store.RetrieveJobAsync(id, cancellation);

        public Task<Partition> RetrievePartitionAsync(Guid id, CancellationToken cancellation)
            => this._store.RetrievePartitionAsync(id, cancellation);

        public Task<Partition> TryAcquirePartitionAsync(string jobId, string requester, DateTime timestamp, DateTime active, CancellationToken cancellation)
            => this._store.TryAcquirePartitionAsync(jobId, requester, timestamp, active, cancellation);

        public Task<bool> TryRequestSplitAsync(string jobId, string requester, DateTime active, CancellationToken cancellation)
            => this._store.TryRequestSplitAsync(jobId, requester, active, cancellation);

        public Task<bool> IsSplitRequestPendingAsync(string jobId, string requester, CancellationToken cancellation)
            => this._store.IsSplitRequestPendingAsync(jobId, requester, cancellation);

        public Task<Job> MarkJobAsReadyAsync(string id, DateTime timestamp, CancellationToken cancellation)
            => this._store.MarkJobAsReadyAsync(id, timestamp, cancellation);

        public Task<Job> MarkJobAsStartedAsync(string id, DateTime timestamp, CancellationToken cancellation)
            => this._store.MarkJobAsStartedAsync(id, timestamp, cancellation);

        public Task<Job> MarkJobAsCompletedAsync(string id, DateTime timestamp, CancellationToken cancellation)
            => this._store.MarkJobAsCompletedAsync(id, timestamp, cancellation);

        public Task<Job> MarkJobAsFailedAsync(string id, DateTime timestamp, string error, CancellationToken cancellation)
            => this._store.MarkJobAsFailedAsync(id, timestamp, error, cancellation);

        public Task<StatusReport> RetrieveJobStatusReportAsync(string id, DateTime timestamp, CancellationToken cancellation)
            => this._store.RetrieveJobStatusReportAsync(id, timestamp, cancellation);
    }
}
