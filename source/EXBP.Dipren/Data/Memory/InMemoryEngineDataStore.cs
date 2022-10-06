
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
        public Task InsertSplitPartitionAsync(Partition partitionToUpdate, Partition partitionToInsert, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(partitionToUpdate, nameof(partitionToUpdate));
            Assert.ArgumentIsNotNull(partitionToInsert, nameof(partitionToInsert));

            lock (this._syncRoot)
            {
                bool partitionToUpdateExists = this._partitions.Contains(partitionToUpdate.Id);

                if (partitionToUpdateExists == false)
                {
                    throw new UnknownIdentifierException(InMemoryEngineDataStoreResources.PartitionWithSpecifiedIdentifierDoesNotExist);
                }

                bool partitionToInsertExists = this._partitions.Contains(partitionToInsert.Id);

                if (partitionToInsertExists == true)
                {
                    throw new UnknownIdentifierException(InMemoryEngineDataStoreResources.PartitionWithSameIdentifierAlreadyExists);
                }

                //
                // Only update fields that are valid to update. E.g. the job identifier cannot be updated.
                //

                Partition updated = this._partitions[partitionToUpdate.Id] with
                {
                    Owner = partitionToInsert.Owner,
                    Updated = partitionToUpdate.Updated,
                    Last = partitionToUpdate.Last,
                    IsInclusive = partitionToUpdate.IsInclusive,
                    Position = partitionToUpdate.Position,
                    Processed = partitionToUpdate.Processed,
                    Remaining = partitionToUpdate.Remaining,
                    IsSplitRequested = partitionToUpdate.IsSplitRequested
                };

                this._partitions.Remove(partitionToUpdate.Id);

                this._partitions.Add(updated);
                this._partitions.Add(partitionToInsert);
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
        {
            Assert.ArgumentIsNotNull(id, nameof(id));

            Partition result = null;

            lock (this._syncRoot)
            {
                try
                {
                    result = this._partitions[id];
                }
                catch (KeyNotFoundException ex)
                {
                    throw new UnknownIdentifierException(InMemoryEngineDataStoreResources.JobWithSpecifiedIdentifierDoesNotExist, ex);
                }
            }

            return Task.FromResult(result);
        }

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
        public Task<Partition> TryAcquirePartitionsAsync(string jobId, string requester, DateTime timestamp, DateTime active, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));
            Assert.ArgumentIsNotNull(requester, nameof(requester));

            Partition result = null;

            lock (this._syncRoot)
            {
                bool exists = this._jobs.Contains(jobId);

                if (exists == false)
                {
                    throw new UnknownIdentifierException(InMemoryEngineDataStoreResources.JobWithSpecifiedIdentifierDoesNotExist);
                }

                Partition current = this._partitions
                    .Where(p => (p.JobId == jobId) && ((p.Owner == null) || (p.Updated < active)) && (p.Remaining > 0L))
                    .OrderByDescending(p => p.Remaining)
                    .FirstOrDefault();

                if (current != null)
                {
                    result = current with
                    {
                        Owner = requester,
                        Updated = timestamp,
                        IsSplitRequested = false
                    };

                    this._partitions.Remove(current.Id);
                    this._partitions.Add(result);
                }
            }

            return Task.FromResult(result);
        }

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
        {
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));

            bool result = false;

            lock (this._syncRoot)
            {
                bool exists = this._jobs.Contains(jobId);

                if (exists == false)
                {
                    throw new UnknownIdentifierException(InMemoryEngineDataStoreResources.JobWithSpecifiedIdentifierDoesNotExist);
                }

                Partition candidate = this._partitions
                    .Where(p => (p.JobId == jobId) && (p.Owner != null) && (p.Updated >= active) && (p.Remaining > 0L) && (p.IsSplitRequested == false))
                    .OrderByDescending(p => p.Remaining)
                    .FirstOrDefault();

                if (candidate != null)
                {
                    Partition updated = candidate with
                    {
                        IsSplitRequested = true
                    };

                    this._partitions.Remove(updated.Id);
                    this._partitions.Add(updated);

                    result = true;
                }
            }

            return Task.FromResult(result);
        }

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
        public Task<Partition> ReportProgressAsync(Guid id, string owner, DateTime timestamp, string position, long progress, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(owner, nameof(owner));
            Assert.ArgumentIsNotNull(position, nameof(position));

            Partition result = null;

            lock (this._syncRoot)
            {
                bool exists = this._partitions.TryGetValue(id, out Partition persisted);

                if (exists == false)
                {
                    throw new UnknownIdentifierException(InMemoryEngineDataStoreResources.PartitionWithSpecifiedIdentifierDoesNotExist);
                }

                if (persisted.Owner != owner)
                {
                    throw new LockException(InMemoryEngineDataStoreResources.PartitionLockNoLongerHeld);
                }

                result = persisted with
                {
                    Updated = timestamp,
                    Position = position,
                    Processed = (persisted.Processed + progress),
                    Remaining = (persisted.Remaining - progress)
                };

                this._partitions.Remove(result.Id);
                this._partitions.Add(result);
            }


            return Task.FromResult(result);
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
