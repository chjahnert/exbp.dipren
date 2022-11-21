
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Diagnostics;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Data.SQLite
{
    /// <summary>
    ///   Implements an <see cref="IEngineDataStore"/> that uses SQLite as its storage engine.
    /// </summary>
    public class SQLiteEngineDataStore : EngineDataStore, IEngineDataStore, IDisposable, IAsyncDisposable
    {
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
        private readonly SQLiteConnection _connection;
        private bool _disposed = false;


        /// <summary>
        ///   Initializes a new and empty instance of the <see cref="SQLiteEngineDataStore"/> class.
        /// </summary>
        /// <param name="connectionString">
        ///   A <see cref="string"/> value that contains the connection string to use.
        /// </param>
        public SQLiteEngineDataStore(string connectionString)
        {
            this._connection = new SQLiteConnection(connectionString);

            this._connection.Open();

            SQLiteEngineDataStore.EnsureForeignKeys(this._connection);
            SQLiteEngineDataStore.EnsureSchema(this._connection);

            this._connection.Busy += this.OnDatabaseBusy;
        }

        /// <summary>
        ///   Handles the event when the underlying database is busy.
        /// </summary>
        /// <param name="sender">
        ///   The object that generated the event.
        /// </param>
        /// <param name="args">
        ///   The event arguments.
        /// </param>
        protected virtual void OnDatabaseBusy(object sender, BusyEventArgs args)
        {
            args.ReturnCode = SQLiteBusyReturnCode.Retry;
        }

        /// <summary>
        ///   Disposes the current object and releases unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);

            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///   Disposes the current object and releases unmanaged resources.
        /// </summary>
        /// <param name="disposing">
        ///   Indicates whether the current method was called from the <see cref="Dispose"/> method or from the
        ///   destructor.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            if (this._disposed == false)
            {
                if (disposing == true)
                {
                    this._connection.Dispose();
                    this._lock.Dispose();
                }

                this._disposed = true;
            }
        }

        /// <summary>
        ///   Asynchronously disposes the current object and releases unmanaged resources.
        /// </summary>
        /// <returns>
        ///   A <see cref="ValueTask"/> representing the asynchronous operation.
        /// </returns>
        public virtual async ValueTask DisposeAsync()
        {
            if (this._disposed == false)
            {
                await this._connection.DisposeAsync();
                this._lock.Dispose();

                this._disposed = true;
            }
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
        public async Task<long> CountJobsAsync(CancellationToken cancellation)
        {
            long result = 0L;

            this._lock.EnterReadLock();

            try
            {
                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.QueryCountJobs,
                    CommandType = CommandType.Text,
                    Connection = this._connection
                };

                result = (long) await command.ExecuteScalarAsync(cancellation);
            }
            finally
            {
                this._lock.ExitReadLock();
            }

            return result;
        }

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
        public async Task<long> CountIncompletePartitionsAsync(string jobId, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));

            long result = 0L;

            this._lock.EnterReadLock();

            try
            {
                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.QueryCountIncompletePartitions,
                    CommandType = CommandType.Text,
                    Connection = this._connection
                };

                command.Parameters.AddWithValue("$job_id", jobId);

                using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
                {
                    await reader.ReadAsync(cancellation);

                    long jobCount = reader.GetInt64("job_count");

                    if (jobCount == 0L)
                    {
                        this.RaiseErrorUnknownJobIdentifier();
                    }

                    result = reader.GetInt64("partition_count");
                }
            }
            finally
            {
                this._lock.ExitReadLock();
            }

            return result;
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
        public async Task InsertJobAsync(Job job, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(job, nameof(job));

            this._lock.EnterWriteLock();

            try
            {
                using SQLiteTransaction transaction = this._connection.BeginTransaction();

                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.QueryInsertJob,
                    CommandType = CommandType.Text,
                    Transaction = transaction
                };

                command.Parameters.AddWithValue("$id", job.Id);
                command.Parameters.AddWithValue("$created", job.Created);
                command.Parameters.AddWithValue("$updated", job.Updated);
                command.Parameters.AddWithValue("$batch_size", job.BatchSize);
                command.Parameters.AddWithValue("$timeout", job.Timeout.Ticks);
                command.Parameters.AddWithValue("$clock_drift", job.ClockDrift.Ticks);
                command.Parameters.AddWithValue("$started", job.Started);
                command.Parameters.AddWithValue("$completed", job.Completed);
                command.Parameters.AddWithValue("$state", job.State);
                command.Parameters.AddWithValue("$error", job.Error);

                try
                {
                    await command.ExecuteNonQueryAsync(cancellation);
                }
                catch (SQLiteException ex) when (ex.ErrorCode == 19)
                {
                    this.RaiseErrorDuplicateJobIdentifier(ex);
                }

                transaction.Commit();
            }
            finally
            {
                this._lock.ExitWriteLock();
            }
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
        public async Task InsertPartitionAsync(Partition partition, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(partition, nameof(partition));

            this._lock.EnterWriteLock();

            try
            {
                using SQLiteTransaction transaction = this._connection.BeginTransaction();

                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.QueryInsertPartition,
                    CommandType = CommandType.Text,
                    Transaction = transaction
                };

                string id = partition.Id.ToString("d");

                command.Parameters.AddWithValue("$id", id);
                command.Parameters.AddWithValue("$job_id", partition.JobId);
                command.Parameters.AddWithValue("$created", partition.Created);
                command.Parameters.AddWithValue("$updated", partition.Updated);
                command.Parameters.AddWithValue("$owner", partition.Owner);
                command.Parameters.AddWithValue("$first", partition.First);
                command.Parameters.AddWithValue("$last", partition.Last);
                command.Parameters.AddWithValue("$is_inclusive", partition.IsInclusive);
                command.Parameters.AddWithValue("$position", partition.Position);
                command.Parameters.AddWithValue("$processed", partition.Processed);
                command.Parameters.AddWithValue("$remaining", partition.Remaining);
                command.Parameters.AddWithValue("$throughput", partition.Throughput);
                command.Parameters.AddWithValue("$is_completed", partition.IsCompleted);
                command.Parameters.AddWithValue("$is_split_requested", partition.IsSplitRequested);

                try
                {
                    await command.ExecuteNonQueryAsync(cancellation);
                }
                catch (SQLiteException ex) when (ex.ErrorCode == 19)
                {
                    bool exists = await this.DoesJobExistAsync(transaction, partition.JobId, cancellation);

                    if (exists == false)
                    {
                        this.RaiseErrorInvalidJobReference();
                    }
                    else
                    {
                        this.RaiseErrorDuplicatePartitionIdentifier(ex);
                    }
                }

                transaction.Commit();
            }
            finally
            {
                this._lock.ExitWriteLock();
            }
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
        public async Task InsertSplitPartitionAsync(Partition partitionToUpdate, Partition partitionToInsert, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(partitionToUpdate, nameof(partitionToUpdate));
            Assert.ArgumentIsNotNull(partitionToInsert, nameof(partitionToInsert));

            this._lock.EnterWriteLock();

            try
            {
                using SQLiteTransaction transaction = this._connection.BeginTransaction();

                {
                    using SQLiteCommand command = new SQLiteCommand
                    {
                        CommandText = SQLiteEngineDataStoreResources.QueryUpdateSplitPartition,
                        CommandType = CommandType.Text,
                        Transaction = transaction
                    };

                    string id = partitionToUpdate.Id.ToString("d");

                    command.Parameters.AddWithValue("$partition_id", id);
                    command.Parameters.AddWithValue("$owner", partitionToUpdate.Owner);
                    command.Parameters.AddWithValue("$updated", partitionToUpdate.Updated);
                    command.Parameters.AddWithValue("$last", partitionToUpdate.Last);
                    command.Parameters.AddWithValue("$is_inclusive", partitionToUpdate.IsInclusive);
                    command.Parameters.AddWithValue("$position", partitionToUpdate.Position);
                    command.Parameters.AddWithValue("$processed", partitionToUpdate.Processed);
                    command.Parameters.AddWithValue("$remaining", partitionToUpdate.Remaining);
                    command.Parameters.AddWithValue("$throughput", partitionToUpdate.Throughput);
                    command.Parameters.AddWithValue("$is_split_requested", partitionToUpdate.IsSplitRequested);

                    int affected = await command.ExecuteNonQueryAsync(cancellation);

                    if (affected != 1)
                    {
                        bool exists = await this.DoesPartitionExistAsync(transaction, partitionToUpdate.Id, cancellation);

                        if (exists == false)
                        {
                            this.RaiseErrorUnknownPartitionIdentifier();
                        }
                        else
                        {
                            this.RaiseErrorLockNoLongerHeld();
                        }
                    }
                }

                {
                    using SQLiteCommand command = new SQLiteCommand
                    {
                        CommandText = SQLiteEngineDataStoreResources.QueryInsertPartition,
                        CommandType = CommandType.Text,
                        Transaction = transaction
                    };

                    string id = partitionToInsert.Id.ToString("d");

                    command.Parameters.AddWithValue("$id", id);
                    command.Parameters.AddWithValue("$job_id", partitionToInsert.JobId);
                    command.Parameters.AddWithValue("$created", partitionToInsert.Created);
                    command.Parameters.AddWithValue("$updated", partitionToInsert.Updated);
                    command.Parameters.AddWithValue("$owner", partitionToInsert.Owner);
                    command.Parameters.AddWithValue("$first", partitionToInsert.First);
                    command.Parameters.AddWithValue("$last", partitionToInsert.Last);
                    command.Parameters.AddWithValue("$is_inclusive", partitionToInsert.IsInclusive);
                    command.Parameters.AddWithValue("$position", partitionToInsert.Position);
                    command.Parameters.AddWithValue("$processed", partitionToInsert.Processed);
                    command.Parameters.AddWithValue("$remaining", partitionToInsert.Remaining);
                    command.Parameters.AddWithValue("$throughput", partitionToInsert.Throughput);
                    command.Parameters.AddWithValue("$is_completed", partitionToInsert.IsCompleted);
                    command.Parameters.AddWithValue("$is_split_requested", partitionToInsert.IsSplitRequested);

                    try
                    {
                        await command.ExecuteNonQueryAsync(cancellation);
                    }
                    catch (SQLiteException ex) when (ex.ErrorCode == 19)
                    {
                        this.RaiseErrorDuplicatePartitionIdentifier(ex);
                    }
                }

                transaction.Commit();
            }
            finally
            {
                this._lock.ExitWriteLock();
            }
        }

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
        public async Task<Job> MarkJobAsReadyAsync(string id, DateTime timestamp, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(id, nameof(id));

            Job result = null;

            this._lock.EnterWriteLock();

            try
            {
                using SQLiteTransaction transaction = this._connection.BeginTransaction();

                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.QueryMarkJobAsReady,
                    CommandType = CommandType.Text,
                    Transaction = transaction
                };

                command.Parameters.AddWithValue("$id", id);
                command.Parameters.AddWithValue("$timestamp", timestamp);
                command.Parameters.AddWithValue("$state", JobState.Ready);

                using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
                {
                    bool found = await reader.ReadAsync(cancellation);

                    if (found == false)
                    {
                        this.RaiseErrorUnknownJobIdentifier();
                    }

                    result = this.ReadJob(reader);
                }

                transaction.Commit();
            }
            finally
            {
                this._lock.ExitWriteLock();
            }

            return result;
        }

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
        public async Task<Job> MarkJobAsStartedAsync(string id, DateTime timestamp, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(id, nameof(id));

            Job result = null;

            this._lock.EnterWriteLock();

            try
            {
                using SQLiteTransaction transaction = this._connection.BeginTransaction();

                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.QueryMarkJobAsStarted,
                    CommandType = CommandType.Text,
                    Transaction = transaction
                };

                command.Parameters.AddWithValue("$id", id);
                command.Parameters.AddWithValue("$timestamp", timestamp);
                command.Parameters.AddWithValue("$state", JobState.Processing);

                using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
                {
                    bool found = await reader.ReadAsync(cancellation);

                    if (found == false)
                    {
                        this.RaiseErrorUnknownJobIdentifier();
                    }

                    result = this.ReadJob(reader);
                }

                transaction.Commit();
            }
            finally
            {
                this._lock.ExitWriteLock();
            }

            return result;
        }

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
        public async Task<Job> MarkJobAsCompletedAsync(string id, DateTime timestamp, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(id, nameof(id));

            Job result = null;

            this._lock.EnterWriteLock();

            try
            {
                using SQLiteTransaction transaction = this._connection.BeginTransaction();

                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.QueryMarkJobAsCompleted,
                    CommandType = CommandType.Text,
                    Transaction = transaction
                };

                command.Parameters.AddWithValue("$id", id);
                command.Parameters.AddWithValue("$timestamp", timestamp);
                command.Parameters.AddWithValue("$state", JobState.Completed);

                using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
                {
                    bool found = await reader.ReadAsync(cancellation);

                    if (found == false)
                    {
                        this.RaiseErrorUnknownJobIdentifier();
                    }

                    result = this.ReadJob(reader);
                }

                transaction.Commit();
            }
            finally
            {
                this._lock.ExitWriteLock();
            }

            return result;
        }

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
        ///   The description of the error that caused the job to fail.
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
        public async Task<Job> MarkJobAsFailedAsync(string id, DateTime timestamp, string error, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(id, nameof(id));

            Job result = null;

            this._lock.EnterWriteLock();

            try
            {
                using SQLiteTransaction transaction = this._connection.BeginTransaction();

                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.QueryMarkJobAsFailed,
                    CommandType = CommandType.Text,
                    Transaction = transaction
                };

                command.Parameters.AddWithValue("$id", id);
                command.Parameters.AddWithValue("$timestamp", timestamp);
                command.Parameters.AddWithValue("$state", JobState.Failed);
                command.Parameters.AddWithValue("$error", error);

                using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
                {
                    bool found = await reader.ReadAsync(cancellation);

                    if (found == false)
                    {
                        this.RaiseErrorUnknownJobIdentifier();
                    }

                    result = this.ReadJob(reader);
                }

                transaction.Commit();
            }
            finally
            {
                this._lock.ExitWriteLock();
            }

            return result;
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
        public async Task<Job> RetrieveJobAsync(string id, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(id, nameof(id));

            Job result = null;

            this._lock.EnterReadLock();

            try
            {
                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.QueryRetrieveJobById,
                    CommandType = CommandType.Text,
                    Connection = this._connection
                };

                command.Parameters.AddWithValue("$id", id);

                using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
                {
                    bool exists = await reader.ReadAsync(cancellation);

                    if (exists == false)
                    {
                        this.RaiseErrorUnknownJobIdentifier();
                    }

                    result = this.ReadJob(reader);
                }
            }
            finally
            {
                this._lock.ExitReadLock();
            }

            return result;
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
        public async Task<Partition> RetrievePartitionAsync(Guid id, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(id, nameof(id));

            Partition result = null;

            this._lock.EnterReadLock();

            try
            {
                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.QueryRetrievePartitionById,
                    CommandType = CommandType.Text,
                    Connection = this._connection
                };

                string sid = id.ToString("d");

                command.Parameters.AddWithValue("$id", sid);

                using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
                {
                    bool exists = await reader.ReadAsync(cancellation);

                    if (exists == false)
                    {
                        this.RaiseErrorUnknownPartitionIdentifier();
                    }

                    result = this.ReadPartition(reader);
                }
            }
            finally
            {
                this._lock.ExitReadLock();
            }

            return result;
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
        public async Task<Partition> TryAcquirePartitionAsync(string jobId, string requester, DateTime timestamp, DateTime active, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));
            Assert.ArgumentIsNotNull(requester, nameof(requester));

            Partition result = null;

            this._lock.EnterWriteLock();

            try
            {
                using SQLiteTransaction transaction = this._connection.BeginTransaction();

                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.QueryTryAcquirePartition,
                    CommandType = CommandType.Text,
                    Transaction = transaction
                };

                command.Parameters.AddWithValue("$job_id", jobId);
                command.Parameters.AddWithValue("$owner", requester);
                command.Parameters.AddWithValue("$updated", timestamp);
                command.Parameters.AddWithValue("$active", active);

                using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
                {
                    bool found = await reader.ReadAsync(cancellation);

                    if (found == true)
                    {
                        result = this.ReadPartition(reader);
                    }
                }

                if (result == null)
                {
                    bool exists = await this.DoesJobExistAsync(transaction, jobId, cancellation);

                    if (exists == false)
                    {
                        this.RaiseErrorUnknownJobIdentifier();
                    }
                }

                transaction.Commit();
            }
            finally
            {
                this._lock.ExitWriteLock();
            }

            return result;
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
        public async Task<bool> TryRequestSplitAsync(string jobId, DateTime active, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));

            bool result = false;

            this._lock.EnterWriteLock();

            try
            {
                using SQLiteTransaction transaction = this._connection.BeginTransaction();

                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.QueryTryRequestSplit,
                    CommandType = CommandType.Text,
                    Transaction = transaction
                };

                command.Parameters.AddWithValue("$job_id", jobId);
                command.Parameters.AddWithValue("$active", active);

                int affected = await command.ExecuteNonQueryAsync(cancellation);

                if (affected == 0)
                {
                    bool exists = await this.DoesJobExistAsync(transaction, jobId, cancellation);

                    if (exists == false)
                    {
                        this.RaiseErrorUnknownJobIdentifier();
                    }
                }

                transaction.Commit();

                result = (affected > 0);
            }
            finally
            {
                this._lock.ExitWriteLock();
            }

            return result;
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
        public async Task<Partition> ReportProgressAsync(Guid id, string owner, DateTime timestamp, string position, long progress, bool completed, double throughput, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(owner, nameof(owner));
            Assert.ArgumentIsNotNull(position, nameof(position));

            Partition result = null;

            this._lock.EnterWriteLock();

            try
            {
                using SQLiteTransaction transaction = this._connection.BeginTransaction();

                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.QueryReportProgress,
                    CommandType = CommandType.Text,
                    Transaction = transaction
                };

                string sid = id.ToString("d");

                command.Parameters.AddWithValue("$updated", timestamp);
                command.Parameters.AddWithValue("$position", position);
                command.Parameters.AddWithValue("$progress", progress);
                command.Parameters.AddWithValue("$completed", completed);
                command.Parameters.AddWithValue("$throughput", throughput);
                command.Parameters.AddWithValue("$id", sid);
                command.Parameters.AddWithValue("$owner", owner);

                using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
                {
                    bool found = await reader.ReadAsync(cancellation);

                    if (found == true)
                    {
                        result = this.ReadPartition(reader);
                    }
                }

                if (result == null)
                {
                    bool exists = await this.DoesPartitionExistAsync(transaction, id, cancellation);

                    if (exists == false)
                    {
                        this.RaiseErrorUnknownJobIdentifier();
                    }
                    else
                    {
                        this.RaiseErrorLockNoLongerHeld();
                    }
                }

                transaction.Commit();
            }
            finally
            {
                this._lock.ExitWriteLock();
            }

            return result;
        }

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
        public async Task<StatusReport> RetrieveJobStatusReportAsync(string id, DateTime timestamp, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(id, nameof(id));

            StatusReport result = null;

            this._lock.EnterReadLock();

            try
            {
                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.QueryRetrieveJobStatusReport,
                    CommandType = CommandType.Text,
                    Connection = this._connection
                };

                command.Parameters.AddWithValue("$id", id);
                command.Parameters.AddWithValue("$timestamp", timestamp);

                using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
                {
                    bool exists = await reader.ReadAsync(cancellation);

                    if (exists == false)
                    {
                        this.RaiseErrorUnknownJobIdentifier();
                    }


                    Job job = this.ReadJob(reader);

                    result = new StatusReport
                    {
                        Id = job.Id,
                        Timestamp = timestamp,
                        Created = job.Created,
                        Updated = job.Updated,
                        BatchSize = job.BatchSize,
                        Timeout = job.Timeout,
                        Started = job.Started,
                        Completed = job.Completed,
                        State = job.State,
                        Error = job.Error,

                        LastActivity = reader.GetDateTime("last_activity"),
                        OwnershipChanges = reader.GetInt64("ownership_changes"),
                        PendingSplitRequests = reader.GetInt64("split_requests_pending"),
                        CurrentThroughput = reader.GetDouble("current_throughput"),

                        Partitions = new StatusReport.PartitionsReport
                        {
                            Untouched = reader.GetInt64("partitons_untouched"),
                            InProgress = reader.GetInt64("partitons_in_progress"),
                            Completed = reader.GetInt64("partitions_completed")
                        },

                        Progress = new StatusReport.ProgressReport
                        {
                            Remaining = reader.GetNullableInt64("keys_remaining"),
                            Completed = reader.GetNullableInt64("keys_completed")
                        }
                    };
                }
            }
            finally
            {
                this._lock.ExitReadLock();
            }

            return result;
        }

        /// <summary>
        ///   Determines if a job with the specified unique identifier exists.
        /// </summary>
        /// <param name="transaction">
        ///   The transaction to participate in.
        /// </param>
        /// <param name="id">
        ///   The unique identifier of the job to check.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="bool"/> object that represents the asynchronous
        ///   operation.
        /// </returns>
        private async Task<bool> DoesJobExistAsync(SQLiteTransaction transaction, string id, CancellationToken cancellation)
        {
            Debug.Assert(id != null);

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.QueryDoesJobExist,
                CommandType = CommandType.Text,
                Transaction = transaction
            };

            command.Parameters.AddWithValue("$id", id);

            long count = (long) await command.ExecuteScalarAsync(cancellation);

            bool result = (count > 0);

            return result;
        }

        /// <summary>
        ///   Determines if a partition with the specified unique identifier exists.
        /// </summary>
        /// <param name="transaction">
        ///   The transaction to participate in.
        /// </param>
        /// <param name="id">
        ///   The unique identifier of the partition to check.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="bool"/> object that represents the asynchronous
        ///   operation.
        /// </returns>
        private async Task<bool> DoesPartitionExistAsync(SQLiteTransaction transaction, Guid id, CancellationToken cancellation)
        {
            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.QueryDoesPartitionExist,
                CommandType = CommandType.Text,
                Transaction = transaction
            };

            string sid = id.ToString("d");

            command.Parameters.AddWithValue("$id", sid);

            long count = (long) await command.ExecuteScalarAsync(cancellation);

            bool result = (count > 0);

            return result;
        }

        /// <summary>
        ///   Constructs a <see cref="Job"/> object from the values read from the current position of the specified
        ///   reader.
        /// </summary>
        /// <param name="reader">
        ///   The <see cref="DbDataReader"/> to read from.
        /// </param>
        /// <returns>
        ///   The <see cref="Job"/> constructed from the values read from the reader.
        /// </returns>
        private Job ReadJob(DbDataReader reader)
        {
            Debug.Assert(reader != null);

            string id = reader.GetString("id");
            DateTime created = reader.GetDateTime("created");
            DateTime updated = reader.GetDateTime("updated");
            int batchSize = reader.GetInt32("batch_size");
            long ticksTimeout = reader.GetInt64("timeout");
            long ticksClockDrift = reader.GetInt64("clock_drift");
            DateTime? started = reader.GetNullableDateTime("started");
            DateTime? completed = reader.GetNullableDateTime("completed");
            JobState state = (JobState) reader.GetInt32("state");
            string error = reader.GetNullableString("error");

            TimeSpan timeout = TimeSpan.FromTicks(ticksTimeout);
            TimeSpan clockDrift = TimeSpan.FromTicks(ticksClockDrift);

            Job result = new Job(id, created, updated, state, batchSize, timeout, clockDrift, started, completed, error);

            return result;
        }

        /// <summary>
        ///   Constructs a <see cref="Partition"/> object from the values read from the current position of the
        ///   specified reader.
        /// </summary>
        /// <param name="reader">
        ///   The <see cref="DbDataReader"/> to read from.
        /// </param>
        /// <returns>
        ///   The <see cref="Partition"/> constructed from the values read from the reader.
        /// </returns>
        private Partition ReadPartition(DbDataReader reader)
        {
            Debug.Assert(reader != null);

            string sid = reader.GetString("id");
            string jobId = reader.GetString("job_id");
            DateTime created = reader.GetDateTime("created");
            DateTime updated = reader.GetDateTime("updated");
            string owner = reader.GetNullableString("owner");
            string first = reader.GetString("first");
            string last = reader.GetString("last");
            bool inclusive = reader.GetBoolean("is_inclusive");
            string position = reader.GetNullableString("position");
            long processed = reader.GetInt64("processed");
            long remaining = reader.GetInt64("remaining");
            double throughput = reader.GetDouble("throughput");
            bool completed = reader.GetBoolean("is_completed");
            bool split = reader.GetBoolean("is_split_requested");

            Guid id = Guid.ParseExact(sid, "d");

            Partition result = new Partition(id, jobId, created, updated, first, last, inclusive, position, processed, remaining, owner, completed, throughput, split);

            return result;
        }

        /// <summary>
        ///   Ensures that foreign keys are enabled on the database.
        /// </summary>
        /// <param name="connection">
        ///   The database connection to use.
        /// </param>
        private static void EnsureForeignKeys(SQLiteConnection connection)
        {
            Debug.Assert(connection != null);

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.QueryEnableForeignKeys,
                CommandType = CommandType.Text,
                Connection = connection
            };

            command.ExecuteNonQuery();
        }

        /// <summary>
        ///   Ensures that the database schema is in place.
        /// </summary>
        /// <param name="connection">
        ///   The database connection to use.
        /// </param>
        private static void EnsureSchema(SQLiteConnection connection)
        {
            Debug.Assert(connection != null);

            using SQLiteCommand commandSchema = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.QueryCreateSchema,
                CommandType = CommandType.Text,
                Connection = connection
            };

            commandSchema.ExecuteNonQuery();
        }
    }
}
