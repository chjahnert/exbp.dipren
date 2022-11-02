
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Diagnostics;

using EXBP.Dipren.Data.SQLite;
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Data.Memory
{
    /// <summary>
    ///   Implements an <see cref="IEngineDataStore"/> that uses SQLite as its storage engine.
    /// </summary>
    public class SQLiteEngineDataStore : EngineDataStore, IEngineDataStore
    {
        private readonly SQLiteConnection _connection;


        /// <summary>
        ///   Initializes a new and empty instance of the <see cref="SQLiteEngineDataStore"/> class.
        /// </summary>
        /// <param name="connection">
        ///   The ready-to-be-used <see cref="SQLiteConnection"/> object.
        /// </param>
        protected SQLiteEngineDataStore(SQLiteConnection connection)
        {
            Assert.ArgumentIsNotNull(connection, nameof(connection));

            this._connection = connection;
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
            throw new NotImplementedException();
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

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.SqlCountIncompletePartitions,
                CommandType = CommandType.Text,
                Connection = _connection
            };

            command.Parameters.AddWithValue("$job_id", jobId);

            using DbDataReader reader = await command.ExecuteReaderAsync(cancellation);

            await reader.ReadAsync(cancellation);

            long jobCount = reader.GetInt64("job_count");

            if (jobCount == 0L)
            {
                this.RaiseErrorUnknownJobIdentifier();
            }

            long result = reader.GetInt64("partition_count");

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

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.SqlInsertJob,
                CommandType = CommandType.Text,
                Connection = _connection
            };

            command.Parameters.AddWithValue("$id", job.Id);
            command.Parameters.AddWithValue("$created", job.Created);
            command.Parameters.AddWithValue("$updated", job.Updated);
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

            using SQLiteTransaction transaction = this._connection.BeginTransaction();

            bool exists = await this.DoesJobExistAsync(transaction, partition.JobId, cancellation);

            if (exists == false)
            {
                this.RaiseErrorInvalidJobReference();
            }

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.SqlInsertPartition,
                CommandType = CommandType.Text,
                Connection = _connection
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
            command.Parameters.AddWithValue("$is_completed", partition.IsCompleted);
            command.Parameters.AddWithValue("$is_split_requested", partition.IsSplitRequested);

            try
            {
                await command.ExecuteNonQueryAsync(cancellation);
            }
            catch (SQLiteException ex) when (ex.ErrorCode == 19)
            {
                this.RaiseErrorDuplicatePartitionIdentifier(ex);
            }

            transaction.Commit();
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

            using SQLiteTransaction transaction = this._connection.BeginTransaction();

            {
                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.SqlInsertPartition,
                    CommandType = CommandType.Text,
                    Connection = _connection,
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

            {
                using SQLiteCommand command = new SQLiteCommand
                {
                    CommandText = SQLiteEngineDataStoreResources.SqlUpdateSplitPartition,
                    CommandType = CommandType.Text,
                    Connection = this._connection,
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
                command.Parameters.AddWithValue("$is_split_requested", partitionToUpdate.IsSplitRequested);

                int affected = await command.ExecuteNonQueryAsync(cancellation);

                if (affected != 1)
                {
                    this.RaiseErrorUnknownPartitionIdentifier();
                }
            }

            transaction.Commit();
        }

        /// <summary>
        ///   Updates the state of an existing job.
        /// </summary>
        /// <param name="jobId">
        ///   The unique identifier of the job to update.
        /// </param>
        /// <param name="timestamp">
        ///   The current date and time value.
        /// </param>
        /// <param name="state">
        ///   The new state of the job.
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
        ///   Argument <paramref name="job"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <exception cref="UnknownIdentifierException">
        ///   A job with the specified unique identifier does not exist in the data store.
        /// </exception>
        public async Task<Job> UpdateJobAsync(string jobId, DateTime timestamp, JobState state, string error, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.SqlUpdateJobById,
                CommandType = CommandType.Text,
                Connection = this._connection
            };

            command.Parameters.AddWithValue("$id", jobId);
            command.Parameters.AddWithValue("$updated", timestamp);
            command.Parameters.AddWithValue("$state", state);
            command.Parameters.AddWithValue("$error", error);

            using DbDataReader reader = await command.ExecuteReaderAsync(cancellation);

            bool found = await reader.ReadAsync(cancellation);

            if (found == false)
            {
                this.RaiseErrorUnknownJobIdentifier();
            }

            string rId = reader.GetString("id");
            DateTime rCreated = reader.GetDateTime("created");
            DateTime rUpdated = reader.GetDateTime("updated");
            JobState rState = (JobState) reader.GetInt32("state");
            string rError = reader.IsDBNull("error") ? null : reader.GetString("error");

            Job result = new Job(rId, rCreated, rUpdated, rState, rError);

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

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.SqlRetrieveJobById,
                CommandType = CommandType.Text,
                Connection = _connection
            };

            command.Parameters.AddWithValue("$id", id);

            using DbDataReader reader = await command.ExecuteReaderAsync(cancellation);

            bool exists = await reader.ReadAsync(cancellation);

            if (exists == false)
            {
                this.RaiseErrorUnknownJobIdentifier();
            }

            string rId = reader.GetString("id");
            DateTime rCreated = reader.GetDateTime("created");
            DateTime rUpdated = reader.GetDateTime("updated");
            JobState rState = (JobState) reader.GetInt32("state");
            string rError = reader.IsDBNull("error") ? null : reader.GetString("error");

            Job result = new Job(rId, rCreated, rUpdated, rState, rError);

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

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.SqlRetrievePartitionById,
                CommandType = CommandType.Text,
                Connection = this._connection
            };

            string sid = id.ToString("d");

            command.Parameters.AddWithValue("$id", sid);

            using DbDataReader reader = await command.ExecuteReaderAsync(cancellation);

            bool exists = await reader.ReadAsync(cancellation);

            if (exists == false)
            {
                this.RaiseErrorUnknownPartitionIdentifier();
            }

            string jobId = reader.GetString("job_id");
            DateTime created = reader.GetDateTime("created");
            DateTime updated = reader.GetDateTime("updated");
            string owner = reader.IsDBNull("owner") ? null : reader.GetString("owner");
            string first = reader.GetString("first");
            string last = reader.GetString("last");
            bool inclusive = reader.GetBoolean("is_inclusive");
            string position = reader.IsDBNull("position") ? null : reader.GetString("position");
            long processed = reader.GetInt64("processed");
            long remaining = reader.GetInt64("remaining");
            bool completed = reader.GetBoolean("is_completed");
            bool split = reader.GetBoolean("is_split_requested");

            Partition result = new Partition(id, jobId, created, updated, first, last, inclusive, position, processed, remaining, owner, completed, split);

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
        public async Task<Partition> TryAcquirePartitionsAsync(string jobId, string requester, DateTime timestamp, DateTime active, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));
            Assert.ArgumentIsNotNull(requester, nameof(requester));

            using SQLiteTransaction transaction = this._connection.BeginTransaction();

            bool exists = await this.DoesJobExistAsync(transaction, jobId, cancellation);

            if (exists == false)
            {
                this.RaiseErrorUnknownJobIdentifier();
            }

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.SqlTryAcquirePartition,
                CommandType = CommandType.Text,
                Connection = this._connection,
                Transaction = transaction
            };

            command.Parameters.AddWithValue("$job_id", jobId);
            command.Parameters.AddWithValue("$owner", requester);
            command.Parameters.AddWithValue("$updated", timestamp);
            command.Parameters.AddWithValue("$active", active);

            Partition result = null;

            using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
            {
                bool found = await reader.ReadAsync(cancellation);

                if (found == true)
                {
                    string rId = reader.GetString("id");
                    string rJobId = reader.GetString("job_id");
                    DateTime created = reader.GetDateTime("created");
                    DateTime updated = reader.GetDateTime("updated");
                    string owner = reader.IsDBNull("owner") ? null : reader.GetString("owner");
                    string first = reader.GetString("first");
                    string last = reader.GetString("last");
                    bool inclusive = reader.GetBoolean("is_inclusive");
                    string position = reader.IsDBNull("position") ? null : reader.GetString("position");
                    long processed = reader.GetInt64("processed");
                    long remaining = reader.GetInt64("remaining");
                    bool completed = reader.GetBoolean("is_completed");
                    bool split = reader.GetBoolean("is_split_requested");

                    Guid id = Guid.Parse(rId);

                    result = new Partition(id, rJobId, created, updated, first, last, inclusive, position, processed, remaining, owner, completed, split);
                }
            }

            transaction.Commit();

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

            using SQLiteTransaction transaction = this._connection.BeginTransaction();

            bool exists = await this.DoesJobExistAsync(transaction, jobId, cancellation);

            if (exists == false)
            {
                this.RaiseErrorUnknownJobIdentifier();
            }

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.SqlTryRequestSplit,
                CommandType = CommandType.Text,
                Connection = this._connection,
                Transaction = transaction
            };

            command.Parameters.AddWithValue("$job_id", jobId);
            command.Parameters.AddWithValue("$active", active);

            int affected = await command.ExecuteNonQueryAsync(cancellation);

            transaction.Commit();

            return (affected > 0);
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
        public async Task<Partition> ReportProgressAsync(Guid id, string owner, DateTime timestamp, string position, long progress, bool completed, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(owner, nameof(owner));
            Assert.ArgumentIsNotNull(position, nameof(position));

            using SQLiteTransaction transaction = this._connection.BeginTransaction();

            bool exists = await this.DoesPartitionExistAsync(transaction, id, cancellation);

            if (exists == false)
            {
                this.RaiseErrorUnknownJobIdentifier();
            }

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.SqlReportProgress,
                CommandType = CommandType.Text,
                Connection = this._connection,
                Transaction = transaction
            };

            string sid = id.ToString("d");

            command.Parameters.AddWithValue("$updated", timestamp);
            command.Parameters.AddWithValue("$position", position);
            command.Parameters.AddWithValue("$progress", progress);
            command.Parameters.AddWithValue("$completed", completed);
            command.Parameters.AddWithValue("$id", sid);
            command.Parameters.AddWithValue("$owner", owner);

            Partition result = null;

            using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
            {
                bool found = await reader.ReadAsync(cancellation);

                if (found == true)
                {
                    string rsId = reader.GetString("id");
                    string rJobId = reader.GetString("job_id");
                    DateTime rCreated = reader.GetDateTime("created");
                    DateTime rUpdated = reader.GetDateTime("updated");
                    string rOwner = reader.IsDBNull("owner") ? null : reader.GetString("owner");
                    string rFirst = reader.GetString("first");
                    string rLast = reader.GetString("last");
                    bool rInclusive = reader.GetBoolean("is_inclusive");
                    string rPosition = reader.IsDBNull("position") ? null : reader.GetString("position");
                    long rProcessed = reader.GetInt64("processed");
                    long rRemaining = reader.GetInt64("remaining");
                    bool rCompleted = reader.GetBoolean("is_completed");
                    bool rSplit = reader.GetBoolean("is_split_requested");

                    Guid rId = Guid.Parse(rsId);

                    result = new Partition(rId, rJobId, rCreated, rUpdated, rFirst, rLast, rInclusive, rPosition, rProcessed, rRemaining, rOwner, rCompleted, rSplit);
                }
                else
                {
                    this.RaiseErrorLockNoLongerHeld();
                }
            }

            transaction.Commit();

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
                CommandText = SQLiteEngineDataStoreResources.SqlDoesJobExist,
                CommandType = CommandType.Text,
                Connection = this._connection,
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
                CommandText = SQLiteEngineDataStoreResources.SqlDoesPartitionExist,
                CommandType = CommandType.Text,
                Connection = this._connection,
                Transaction = transaction
            };

            string sid = id.ToString("d");

            command.Parameters.AddWithValue("$id", sid);

            long count = (long) await command.ExecuteScalarAsync(cancellation);

            bool result = (count > 0);

            return result;
        }

        /// <summary>
        ///   Opens the specified database connection and deploys the required table structure.
        /// </summary>
        /// <param name="connectionString">
        ///   A <see cref="string"/> value that contains the connection string to use.
        /// </param>
        /// <returns>
        ///   The <see cref="SQLiteEngineDataStore"/> object that is ready to be used.
        /// </returns>
        public static async Task<SQLiteEngineDataStore> OpenAsync(string connectionString, CancellationToken cancellation)
        {
            SQLiteConnection connection = new SQLiteConnection(connectionString);

            await connection.OpenAsync(cancellation);

            await SQLiteEngineDataStore.EnsureForeignKeysAsync(connection, cancellation);
            await SQLiteEngineDataStore.EnsureSchemaAsync(connection, cancellation);

            SQLiteEngineDataStore result = new SQLiteEngineDataStore(connection);

            return result;
        }

        /// <summary>
        ///   Ensures that foreign keys are enabled on the database.
        /// </summary>
        /// <param name="connection">
        ///   The database connection to use.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> object that represents the asynchronous operation.
        /// </returns>
        protected static async Task EnsureForeignKeysAsync(SQLiteConnection connection, CancellationToken cancellation)
        {
            Debug.Assert(connection != null);

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.SqlEnableForeignKeys,
                CommandType = CommandType.Text,
                Connection = connection
            };

            await command.ExecuteNonQueryAsync(cancellation);
        }

        /// <summary>
        ///   Ensures that the database schema is in place.
        /// </summary>
        /// <param name="connection">
        ///   The database connection to use.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> object that represents the asynchronous operation.
        /// </returns>
        protected static async Task EnsureSchemaAsync(SQLiteConnection connection, CancellationToken cancellation)
        {
            Debug.Assert(connection != null);

            using SQLiteCommand commandSchema = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.SqlCreateSchema,
                CommandType = CommandType.Text,
                Connection = connection
            };

            await commandSchema.ExecuteNonQueryAsync(cancellation);
        }
    }
}
