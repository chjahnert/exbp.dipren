
using EXBP.Dipren.Resilience;


namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   Implements an <see cref="IEngineDataStore"/> that uses the specified retry strategy to ensure resilience.
    /// </summary>
    public abstract class ResilientEngineDataStore : IEngineDataStore
    {
        /// <summary>
        ///   Gets the engine data store instance being wrapped.
        /// </summary>
        /// <value>
        ///   The <see cref="IEngineDataStore"/> instance being wrapped.
        /// </value>
        protected abstract IEngineDataStore Store { get; }

        /// <summary>
        ///   Gets the <see cref="IAsyncRetryStrategy"/> object that implements the retry strategy to use.
        /// </summary>
        /// <value>
        ///   The <see cref="IAsyncRetryStrategy"/> instance that implements the retry strategy to use.
        /// </value>
        protected abstract IAsyncRetryStrategy Strategy { get; }


        /// <summary>
        ///   Initializes a new instance of the <see cref="ResilientEngineDataStore"/> class.
        /// </summary>
        protected ResilientEngineDataStore()
        {
        }


        /// <summary>
        ///   Returns the number of distributed processing jobs in the current data store.
        /// </summary>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> or <see cref="long"/> that represents the asynchronous operation and can
        ///   be used to access the result.
        /// </returns>
        public Task<long> CountJobsAsync(CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.CountJobsAsync(cancellation), cancellation);

        /// <summary>
        ///   Returns the number of incomplete partitions for the specified job.
        /// </summary>
        /// <param name="jobId">
        ///   The unique identifier of the job for which to retrieve the number of incomplete partitions.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> or <see cref="long"/> that represents the asynchronous operation and can
        ///   be used to access the result.
        /// </returns>
        public Task<long> CountIncompletePartitionsAsync(string jobId, CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.CountIncompletePartitionsAsync(jobId, cancellation), cancellation);

        /// <summary>
        ///   Inserts a new job entry into the data store.
        /// </summary>
        /// <param name="job">
        ///   The job entry to insert.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> object that represents the asynchronous operation.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="job"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <exception cref="DuplicateIdentifierException">
        ///   A job with the specified unique identifier already exists in the data store.
        /// </exception>
        public Task InsertJobAsync(Job job, CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.InsertJobAsync(job, cancellation), cancellation);

        /// <summary>
        ///   Inserts a new partition entry into the data store.
        /// </summary>
        /// <param name="partition">
        ///   The new partition entry to insert.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> object that represents the asynchronous operation.
        /// </returns>
        /// <exception cref="DuplicateIdentifierException">
        ///   A partition with the specified unique identifier already exists in the data store.
        /// </exception>
        /// <exception cref="InvalidReferenceException">
        ///   The job referenced by the partition does not exist within the data store.
        /// </exception>
        public Task InsertPartitionAsync(Partition partition, CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.InsertPartitionAsync(partition, cancellation), cancellation);

        /// <summary>
        ///   Inserts a split off partition while updating the split partition as an atomic operation.
        /// </summary>
        /// <param name="partitionToUpdate">
        ///   The partition to update.
        /// </param>
        /// <param name="partitionToInsert">
        ///   The partition to insert.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> object that represents the asynchronous operation.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="partitionToUpdate"/> or argument <paramref name="partitionToInsert"/> is a
        ///   <see langword="null"/> reference.
        /// </exception>
        /// <exception cref="UnknownIdentifierException">
        ///   The partition to update does not exist in the data store.
        /// </exception>
        /// <exception cref="DuplicateIdentifierException">
        ///   The partition to insert already exists in the data store.
        /// </exception>
        public Task InsertSplitPartitionAsync(Partition partitionToUpdate, Partition partitionToInsert, CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.InsertSplitPartitionAsync(partitionToUpdate, partitionToInsert, cancellation), cancellation);

        /// <summary>
        ///   Updates a partition with the progress made.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the partition.
        /// </param>
        /// <param name="owner">
        ///   The unique identifier of the processing node reporting the progress.
        /// </param>
        /// <param name="timestamp">
        ///   The current timestamp.
        /// </param>
        /// <param name="position">
        ///   The key of the last item processed in the key range of the partition.
        /// </param>
        /// <param name="progress">
        ///   The number of items processed since the last progress update.
        /// </param>
        /// <param name="completed">
        ///   <see langword="true"/> if the partition is completed; otherwise, <see langword="false"/>.
        /// </param>
        /// <param name="throughput">
        ///   The number of items processed per second.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Partition"/> object that represents the asynchronous
        ///   operation. The <see cref="Task{TResult}.Result"/> property contains the updated partition.
        /// </returns>
        /// <exception cref="LockException">
        ///   The specified <paramref name="owner"/> no longer holds the lock on the partition.
        /// </exception>
        /// <exception cref="UnknownIdentifierException">
        ///   A partition with the specified unique identifier does not exist.
        /// </exception>
        public Task<Partition> ReportProgressAsync(Guid id, string owner, DateTime timestamp, string position, long progress, bool completed, double throughput, CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.ReportProgressAsync(id, owner, timestamp, position, progress, completed, throughput, cancellation), cancellation);

        /// <summary>
        ///   Retrieves the job with the specified identifier from the data store.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the job.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Job"/> object that represents the asynchronous operation.
        /// </returns>
        public Task<Job> RetrieveJobAsync(string id, CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.RetrieveJobAsync(id, cancellation), cancellation);

        /// <summary>
        ///   Retrieves the partition with the specified identifier from the data store.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the partition.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Partition"/> object that represents the asynchronous
        ///   operation.
        /// </returns>
        public Task<Partition> RetrievePartitionAsync(Guid id, CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.RetrievePartitionAsync(id, cancellation), cancellation);

        /// <summary>
        ///   Tries to acquire a free or abandoned partition.
        /// </summary>
        /// <param name="jobId">
        ///   The unique identifier of the distributed processing job.
        /// </param>
        /// <param name="requester">
        ///   The identifier of the processing node trying to acquire a partition.
        /// </param>
        /// <param name="timestamp">
        ///   The current timestamp.
        /// </param>
        /// <param name="active">
        ///   A <see cref="DateTime"/> value that is used to determine if a partition is actively being processed.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Partition"/> object that represents the asynchronous
        ///   operation. The <see cref="Task{TResult}.Result"/> property contains the acquired partition if succeeded;
        ///   otherwise, <see langword="null"/>.
        /// </returns>
        /// <exception cref="UnknownIdentifierException">
        ///   A job with the specified unique identifier does not exist in the data store.
        /// </exception>
        public Task<Partition> TryAcquirePartitionAsync(string jobId, string requester, DateTime timestamp, DateTime active, CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.TryAcquirePartitionAsync(jobId, requester, timestamp, active, cancellation), cancellation);

        /// <summary>
        ///   Requests an existing partition to be split.
        /// </summary>
        /// <param name="jobId">
        ///   The unique identifier of the distributed processing job.
        /// </param>
        /// <param name="active">
        ///   A <see cref="DateTime"/> value that is used to determine whether a partition is being processed.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="bool"/> object that represents the asynchronous
        ///   operation. The <see cref="Task{TResult}.Result"/> property contains a value indicating whether a split
        ///   was requested.
        /// </returns>
        /// <exception cref="UnknownIdentifierException">
        ///   A job with the specified unique identifier does not exist in the data store.
        /// </exception>
        public Task<bool> TryRequestSplitAsync(string jobId, DateTime active, CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.TryRequestSplitAsync(jobId, active, cancellation), cancellation);

        /// <summary>
        ///   Marks a job as ready.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the job to update.
        /// </param>
        /// <param name="timestamp">
        ///   The current date and time value.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Job"/> object that represents the asynchronous operation and
        ///   provides access to the result of the operation.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="id"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <exception cref="UnknownIdentifierException">
        ///   A job with the specified unique identifier does not exist in the data store.
        /// </exception>
        public Task<Job> MarkJobAsReadyAsync(string id, DateTime timestamp, CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.MarkJobAsReadyAsync(id, timestamp, cancellation), cancellation);

        /// <summary>
        ///   Marks a job as started.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the job to update.
        /// </param>
        /// <param name="timestamp">
        ///   The current date and time value.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Job"/> object that represents the asynchronous operation and
        ///   provides access to the result of the operation.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="id"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <exception cref="UnknownIdentifierException">
        ///   A job with the specified unique identifier does not exist in the data store.
        /// </exception>
        public Task<Job> MarkJobAsStartedAsync(string id, DateTime timestamp, CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.MarkJobAsStartedAsync(id, timestamp, cancellation), cancellation);

        /// <summary>
        ///   Marks a job as completed.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the job to update.
        /// </param>
        /// <param name="timestamp">
        ///   The current date and time value.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Job"/> object that represents the asynchronous operation and
        ///   provides access to the result of the operation.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="id"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <exception cref="UnknownIdentifierException">
        ///   A job with the specified unique identifier does not exist in the data store.
        /// </exception>
        public Task<Job> MarkJobAsCompletedAsync(string id, DateTime timestamp, CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.MarkJobAsCompletedAsync(id, timestamp, cancellation), cancellation);

        /// <summary>
        ///   Marks a job as failed.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the job to update.
        /// </param>
        /// <param name="timestamp">
        ///   The current date and time value.
        /// </param>
        /// <param name="error">
        ///   The description of the error that caused the job to fail; or <see langword="null"/> if not available.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Job"/> object that represents the asynchronous operation and
        ///   provides access to the result of the operation.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="id"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <exception cref="UnknownIdentifierException">
        ///   A job with the specified unique identifier does not exist in the data store.
        /// </exception>
        public Task<Job> MarkJobAsFailedAsync(string id, DateTime timestamp, string error, CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.MarkJobAsFailedAsync(id, timestamp, error, cancellation), cancellation);

        /// <summary>
        ///   Gets a status report for the job with the specified identifier.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the job.
        /// </param>
        /// <param name="timestamp">
        ///   The current date and time, expressed in UTC time.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Job"/> object that represents the asynchronous operation and
        ///   provides access to the result of the operation.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="id"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <exception cref="UnknownIdentifierException">
        ///   A job with the specified unique identifier does not exist in the data store.
        /// </exception>
        public Task<StatusReport> RetrieveJobStatusReportAsync(string id, DateTime timestamp, CancellationToken cancellation)
            => this.Strategy.ExecuteAsync(async () => await this.Store.RetrieveJobStatusReportAsync(id, timestamp, cancellation), cancellation);
    }
}
