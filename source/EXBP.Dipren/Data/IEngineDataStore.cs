namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   Allows a class to implement a data store for the distributed processing engine.
    /// </summary>
    public interface IEngineDataStore
    {
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
        Task<long> CountJobsAsync(CancellationToken cancellation);

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
        /// <exception cref="DuplicateIdentifierException">
        ///   A job with the specified unique identifier already exists in the store.
        /// </exception>
        Task InsertJobAsync(Job job, CancellationToken cancellation);

        /// <summary>
        ///   Updates an existing job entry in the data store.
        /// </summary>
        /// <param name="job">
        ///   The job entry to update.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> object that represents the asynchronous operation.
        /// </returns>
        Task UpdateJobAsync(Job job, CancellationToken cancellation);

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
        Task<Job> RetrieveJobAsync(string id, CancellationToken cancellation);

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
        Task InsertPartitionAsync(Partition partition, CancellationToken cancellation);

        /// <summary>
        ///   Acquires a free partition if it exists.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the distributed processing job.
        /// </param>
        /// <param name="node">
        ///   The identifier of the processing node.
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
        Task<Partition> TryAcquireFreePartitionsAsync(string id, string node, CancellationToken cancellation);

        /// <summary>
        ///   Acquires an abandoned partition if it exists.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the distributed processing job.
        /// </param>
        /// <param name="node">
        ///   The identifier of the processing node.
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
        Task<Partition> TryAcquireAbandonedPartitionAsync(string id, string node, CancellationToken cancellation);

        /// <summary>
        ///   Requests an existing partition to be split.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the distributed processing job.
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
        Task<bool> RequestSplitAsync(string id, CancellationToken cancellation);
    }
}
