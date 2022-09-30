
using System.Collections.ObjectModel;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Data.Memory
{
    /// <summary>
    ///   Implements an in-memory <see cref="IEngineDataStore"/> that can be used for testing.
    /// </summary>
    public class InMemoryEngineDataStore : IEngineDataStore
    {
        private readonly object _syncRoot = new object();
        private readonly JobCollection _jobs = new JobCollection();
        private readonly PartitionCollection _partitions = new PartitionCollection();


        /// <summary>
        ///   Gets the read-only collection of jobs held by the current data store.
        /// </summary>
        /// <value>
        ///   A <see cref="IReadOnlyCollection{T}"/> of <see cref="Job"/> records containing all jobs in the current
        ///   data store. 
        /// </value>
        internal IReadOnlyCollection<Job> Jobs => this._jobs;

        /// <summary>
        ///   Gets the read-only collection of partitions held by the current data store.
        /// </summary>
        /// <value>
        ///   A <see cref="IReadOnlyCollection{T}"/> of <see cref="Partition"/> records containing all jobs in the
        ///   current data store. 
        /// </value>
        internal IReadOnlyCollection<Partition> Partitions => this._partitions;


        /// <summary>
        ///   Initializes a new and empty instance of the <see cref="InMemoryEngineDataStore"/> class.
        /// </summary>
        public InMemoryEngineDataStore()
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
        {
            long result = 0L;

            lock (this._syncRoot)
            {
                result = this._jobs.Count;
            }

            return Task.FromResult(result);
        }

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
        {
            Assert.ArgumentIsNotNull(job, nameof(job));

            lock (this._syncRoot)
            {
                bool exists = this._jobs.Contains(job.Id);

                if (exists == true)
                {
                    throw new DuplicateIdentifierException(InMemoryEngineDataStoreResources.JobWithSameIdentiferAlreadyExists);
                }

                this._jobs.Add(job);
            }

            return Task.CompletedTask;
        }

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
        {
            Assert.ArgumentIsNotNull(partition, nameof(partition));

            lock (this._syncRoot)
            {
                if (this._partitions.Contains(partition.Id) == true)
                {
                    throw new DuplicateIdentifierException(InMemoryEngineDataStoreResources.PartitionWithSameIdentifierAlreadyExists);
                }

                if (this._jobs.Contains(partition.JobId) == false)
                {
                    throw new InvalidReferenceException(InMemoryEngineDataStoreResources.ReferencedJobDoesNotExist);
                }

                this._partitions.Add(partition);
            }

            return Task.CompletedTask;
        }

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
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="job"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <exception cref="UnknownIdentifierException">
        ///   A job with the specified unique identifier does not exist in the data store.
        /// </exception>
        public Task UpdateJobAsync(Job job, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(job, nameof(job));

            lock (this._syncRoot)
            {
                bool exists = this._jobs.Contains(job.Id);

                if (exists == false)
                {
                    throw new UnknownIdentifierException(InMemoryEngineDataStoreResources.JobWithSpecifiedIdentifierDoesNotExist);
                }

                this._jobs.Remove(job.Id);
                this._jobs.Add(job);
            }

            return Task.CompletedTask;
        }

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
        {
            Assert.ArgumentIsNotNull(id, nameof(id));

            Job result = null;

            lock (this._syncRoot)
            {
                try
                {
                    result = this._jobs[id];
                }
                catch (KeyNotFoundException ex)
                {
                    throw new UnknownIdentifierException(InMemoryEngineDataStoreResources.JobWithSpecifiedIdentifierDoesNotExist, ex);
                }
            }

            return Task.FromResult(result);
        }

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
        public Task<Partition> TryAcquireFreePartitionsAsync(string id, string node, CancellationToken cancellation)
        {
            throw new NotImplementedException();
        }

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
        public Task<Partition> TryAcquireAbandonedPartitionAsync(string id, string node, CancellationToken cancellation)
        {
            throw new NotImplementedException();
        }

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
        public Task<bool> RequestSplitAsync(string id, CancellationToken cancellation)
        {
            throw new NotImplementedException();
        }


        /// <summary>
        ///   Implements a collection of <see cref="Partition"/> records.
        /// </summary>
        private class PartitionCollection : KeyedCollection<Guid, Partition>
        {
            /// <summary>
            ///   Initializes a new and empty instance of the <see cref="PartitionCollection"/> type.
            /// </summary>
            public PartitionCollection()
            {
            }

            /// <summary>
            ///   Returns the key for the specified item.
            /// </summary>
            /// <param name="item">
            ///   The item for which to return the key.
            /// </param>
            /// <returns>
            ///   The key of the specified item.
            /// </returns>
            protected override Guid GetKeyForItem(Partition item)
                => item.Id;
        }

        /// <summary>
        ///   Implements a collection of <see cref="Job"/> records.
        /// </summary>
        private class JobCollection : KeyedCollection<string, Job>
        {
            /// <summary>
            ///   Initializes a new and empty instance of the <see cref="JobCollection"/> type.
            /// </summary>
            public JobCollection()
            {
            }

            /// <summary>
            ///   Returns the key for the specified item.
            /// </summary>
            /// <param name="item">
            ///   The item for which to return the key.
            /// </param>
            /// <returns>
            ///   The key of the specified item.
            /// </returns>
            protected override string GetKeyForItem(Job item)
                => item.Id;
        }
    }
}
