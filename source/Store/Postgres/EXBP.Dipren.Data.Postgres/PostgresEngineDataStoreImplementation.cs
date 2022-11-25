
using System.Data;
using System.Data.Common;
using System.Diagnostics;

using EXBP.Dipren.Diagnostics;

using Npgsql;
using NpgsqlTypes;


namespace EXBP.Dipren.Data.Postgres
{
    /// <summary>
    ///   Implements an <see cref="IEngineDataStore"/> that uses Postgres SQL as its storage engine.
    /// </summary>
    /// <remarks>
    ///   The implementation assumes that the required schema and table structure is already deployed.
    /// </remarks>
    internal class PostgresEngineDataStoreImplementation : EngineDataStore, IEngineDataStore, IDisposable, IAsyncDisposable
    {
        private const string SQL_STATE_FOREIGN_KEY_VIOLATION = "23503";
        private const string SQL_STATE_PRIMARY_KEY_VIOLATION = "23505";

        private const string CONSTRAINT_PK_JOBS = "pk_jobs";
        private const string CONSTRAINT_PK_PARTITIONS = "pk_partitions";
        private const string CONSTRAINT_FK_PARTITIONS_TO_JOB = "fk_partitions_to_job";

        private const int COLUMN_JOB_NAME_LENGTH = 256;
        private const int COLUMN_PARTITION_ID_LENGTH = 36;
        private const int COLUMN_PARTITION_OWNER_LENGTH = 256;


        private readonly NpgsqlDataSource _dataSource;


        /// <summary>
        ///   Initializes a new instance of the <see cref="PostgresEngineDataStoreImplementation"/> class.
        /// </summary>
        /// <param name="connectionString">
        ///   The connection string to use when connecting to the database.
        /// </param>
        internal PostgresEngineDataStoreImplementation(string connectionString)
        {
            NpgsqlDataSourceBuilder builder = new NpgsqlDataSourceBuilder(connectionString);

            builder.MapEnum<JobState>();

            this._dataSource = builder.Build();
        }


        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            this._dataSource.Dispose();

            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources asynchronously.
        /// </summary>
        /// <returns>
        ///   A <see cref="ValueTask"/> value that represents the asynchronous dispose operation.
        /// </returns>
        public ValueTask DisposeAsync()
        {
            ValueTask result = this._dataSource.DisposeAsync();

            GC.SuppressFinalize(this);

            return result;
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
            await using NpgsqlCommand command = this._dataSource.CreateCommand(PostgresEngineDataStoreImplementationResources.QueryCountJobs);

            long result = (long) await command.ExecuteScalarAsync(cancellation);

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

            await using NpgsqlCommand command = this._dataSource.CreateCommand(PostgresEngineDataStoreImplementationResources.QueryCountIncompletePartitions);

            command.Parameters.AddWithValue("@job_id", NpgsqlDbType.Varchar, COLUMN_JOB_NAME_LENGTH, jobId);

            await using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
            {
                await reader.ReadAsync(cancellation);

                long jobCount = reader.GetInt64("job_count");

                if (jobCount == 0L)
                {
                    this.RaiseErrorUnknownJobIdentifier();
                }

                result = reader.GetInt64("partition_count");
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

            await using NpgsqlCommand command = this._dataSource.CreateCommand(PostgresEngineDataStoreImplementationResources.QueryInsertJob);

            DateTime uktsCreated = DateTime.SpecifyKind(job.Created, DateTimeKind.Unspecified);
            DateTime uktsUpdated = DateTime.SpecifyKind(job.Updated, DateTimeKind.Unspecified);
            object uktsStarted = ((job.Started != null) ? DateTime.SpecifyKind(job.Started.Value, DateTimeKind.Unspecified) : DBNull.Value);
            object uktsCompleted = ((job.Completed != null) ? DateTime.SpecifyKind(job.Completed.Value, DateTimeKind.Unspecified) : DBNull.Value);
            object error = ((job.Error != null) ? job.Error : DBNull.Value);

            command.Parameters.AddWithValue("@id", NpgsqlDbType.Varchar, COLUMN_JOB_NAME_LENGTH, job.Id);
            command.Parameters.AddWithValue("@created", NpgsqlDbType.Timestamp, uktsCreated);
            command.Parameters.AddWithValue("@updated", NpgsqlDbType.Timestamp, uktsUpdated);
            command.Parameters.AddWithValue("@batch_size", NpgsqlDbType.Integer, job.BatchSize);
            command.Parameters.AddWithValue("@timeout", NpgsqlDbType.Bigint, job.Timeout.Ticks);
            command.Parameters.AddWithValue("@clock_drift", NpgsqlDbType.Bigint, job.ClockDrift.Ticks);
            command.Parameters.AddWithValue("@started", NpgsqlDbType.Timestamp, uktsStarted);
            command.Parameters.AddWithValue("@completed", NpgsqlDbType.Timestamp, uktsCompleted);
            command.Parameters.AddWithValue("@state", job.State);
            command.Parameters.AddWithValue("@error", NpgsqlDbType.Text, error);

            try
            {
                await command.ExecuteNonQueryAsync(cancellation);
            }
            catch (PostgresException ex) when ((ex.SqlState == SQL_STATE_PRIMARY_KEY_VIOLATION) && (ex.ConstraintName == CONSTRAINT_PK_JOBS))
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

            await using NpgsqlConnection connection = await this._dataSource.OpenConnectionAsync(cancellation);

            await this.InsertPartitionAsync(connection, null, partition, cancellation);
        }

        /// <summary>
        ///   Inserts a new partition entry into the data store.
        /// </summary>
        /// <param name="connection">
        ///   The open database connection to use.
        /// </param>
        /// <param name="transaction">
        ///   The active transaction to use.
        /// </param>
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
        private async Task InsertPartitionAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Partition partition, CancellationToken cancellation)
        {
            Debug.Assert(connection != null);
            Debug.Assert(partition != null);

            await using NpgsqlCommand command = new NpgsqlCommand
            {
                CommandText = PostgresEngineDataStoreImplementationResources.QueryInsertPartition,
                CommandType = CommandType.Text,
                Connection = connection,
                Transaction = transaction
            };

            string id = partition.Id.ToString("d");
            DateTime uktsCreated = DateTime.SpecifyKind(partition.Created, DateTimeKind.Unspecified);
            DateTime uktsUpdated = DateTime.SpecifyKind(partition.Updated, DateTimeKind.Unspecified);

            command.Parameters.AddWithValue("@id", NpgsqlDbType.Char, COLUMN_PARTITION_ID_LENGTH, id);
            command.Parameters.AddWithValue("@job_id", NpgsqlDbType.Varchar, COLUMN_JOB_NAME_LENGTH, partition.JobId);
            command.Parameters.AddWithValue("@created", NpgsqlDbType.Timestamp, uktsCreated);
            command.Parameters.AddWithValue("@updated", NpgsqlDbType.Timestamp, uktsUpdated);
            command.Parameters.AddWithValue("@owner", NpgsqlDbType.Varchar, COLUMN_PARTITION_OWNER_LENGTH, ((object) partition.Owner) ?? DBNull.Value);
            command.Parameters.AddWithValue("@first", NpgsqlDbType.Text, partition.First);
            command.Parameters.AddWithValue("@last", NpgsqlDbType.Text, partition.Last);
            command.Parameters.AddWithValue("@is_inclusive", NpgsqlDbType.Boolean, partition.IsInclusive);
            command.Parameters.AddWithValue("@position", NpgsqlDbType.Text, ((object) partition.Position) ?? DBNull.Value);
            command.Parameters.AddWithValue("@processed", NpgsqlDbType.Bigint, partition.Processed);
            command.Parameters.AddWithValue("@remaining", NpgsqlDbType.Bigint, partition.Remaining);
            command.Parameters.AddWithValue("@throughput", NpgsqlDbType.Double, partition.Throughput);
            command.Parameters.AddWithValue("@is_completed", NpgsqlDbType.Boolean, partition.IsCompleted);
            command.Parameters.AddWithValue("@is_split_requested", NpgsqlDbType.Boolean, partition.IsSplitRequested);

            try
            {
                await command.ExecuteNonQueryAsync(cancellation);
            }
            catch (PostgresException ex) when ((ex.SqlState == SQL_STATE_FOREIGN_KEY_VIOLATION) && (ex.ConstraintName == CONSTRAINT_FK_PARTITIONS_TO_JOB))
            {
                this.RaiseErrorInvalidJobReference();
            }
            catch (PostgresException ex) when ((ex.SqlState == SQL_STATE_PRIMARY_KEY_VIOLATION) && (ex.ConstraintName == CONSTRAINT_PK_PARTITIONS))
            {
                this.RaiseErrorDuplicatePartitionIdentifier(ex);
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

            await using (NpgsqlConnection connection = await this._dataSource.OpenConnectionAsync(cancellation))
            {
                await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellation);

                await using NpgsqlCommand command = new NpgsqlCommand
                {
                    CommandText = PostgresEngineDataStoreImplementationResources.QueryUpdateSplitPartition,
                    CommandType = CommandType.Text,
                    Connection = connection,
                    Transaction = transaction
                };

                string id = partitionToUpdate.Id.ToString("d");
                DateTime uktsUpdated = DateTime.SpecifyKind(partitionToUpdate.Updated, DateTimeKind.Unspecified);

                command.Parameters.AddWithValue("@partition_id", NpgsqlDbType.Char, COLUMN_PARTITION_ID_LENGTH, id);
                command.Parameters.AddWithValue("@owner", NpgsqlDbType.Varchar, COLUMN_PARTITION_OWNER_LENGTH, ((object) partitionToUpdate.Owner) ?? DBNull.Value);
                command.Parameters.AddWithValue("@updated", NpgsqlDbType.Timestamp, uktsUpdated);
                command.Parameters.AddWithValue("@last", NpgsqlDbType.Text, partitionToUpdate.Last);
                command.Parameters.AddWithValue("@is_inclusive", NpgsqlDbType.Boolean, partitionToUpdate.IsInclusive);
                command.Parameters.AddWithValue("@position", NpgsqlDbType.Text, ((object) partitionToUpdate.Position) ?? DBNull.Value);
                command.Parameters.AddWithValue("@processed", NpgsqlDbType.Bigint, partitionToUpdate.Processed);
                command.Parameters.AddWithValue("@throughput", NpgsqlDbType.Double, partitionToUpdate.Throughput);
                command.Parameters.AddWithValue("@remaining", NpgsqlDbType.Bigint, partitionToUpdate.Remaining);
                command.Parameters.AddWithValue("@is_split_requested", NpgsqlDbType.Boolean, partitionToUpdate.IsSplitRequested);

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

                await this.InsertPartitionAsync(connection, transaction, partitionToInsert, cancellation);

                transaction.Commit();
            }
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
        /// <param name="processed">
        ///   The total number of items processed in this partition.
        /// </param>
        /// <param name="remaining">
        ///   The total number of items remaining in this partition.
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
        public async Task<Partition> ReportProgressAsync(Guid id, string owner, DateTime timestamp, string position, long processed, long remaining, bool completed, double throughput, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(owner, nameof(owner));
            Assert.ArgumentIsNotNull(position, nameof(position));

            Partition result = null;

            await using (NpgsqlConnection connection = await this._dataSource.OpenConnectionAsync(cancellation))
            {
                await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellation);

                await using NpgsqlCommand command = new NpgsqlCommand
                {
                    CommandText = PostgresEngineDataStoreImplementationResources.QueryReportProgress,
                    CommandType = CommandType.Text,
                    Connection = connection,
                    Transaction = transaction
                };

                string sid = id.ToString("d");
                DateTime uktsTimestamp = DateTime.SpecifyKind(timestamp, DateTimeKind.Unspecified);

                command.Parameters.AddWithValue("@updated", NpgsqlDbType.Timestamp, uktsTimestamp);
                command.Parameters.AddWithValue("@position", NpgsqlDbType.Text, ((object) position) ?? DBNull.Value);
                command.Parameters.AddWithValue("@processed", NpgsqlDbType.Bigint, processed);
                command.Parameters.AddWithValue("@remaining", NpgsqlDbType.Bigint, remaining);
                command.Parameters.AddWithValue("@completed", NpgsqlDbType.Boolean, completed);
                command.Parameters.AddWithValue("@id", NpgsqlDbType.Char, COLUMN_PARTITION_ID_LENGTH, sid);
                command.Parameters.AddWithValue("@owner", NpgsqlDbType.Varchar, COLUMN_PARTITION_OWNER_LENGTH, owner);
                command.Parameters.AddWithValue("@throughput", NpgsqlDbType.Double, throughput);

                await using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
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
        /// <exception cref="UnknownIdentifierException">
        ///   A job with the specified unique identifier does not exist.
        /// </exception>
        public async Task<Job> RetrieveJobAsync(string id, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(id, nameof(id));

            await using NpgsqlCommand command = this._dataSource.CreateCommand(PostgresEngineDataStoreImplementationResources.QueryRetrieveJobById);

            command.Parameters.AddWithValue("@id", NpgsqlDbType.Varchar, 256, id);

            Job result = null;

            await using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
            {
                bool exists = await reader.ReadAsync(cancellation);

                if (exists == false)
                {
                    this.RaiseErrorUnknownJobIdentifier();
                }

                result = this.ReadJob(reader);
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
        /// <exception cref="UnknownIdentifierException">
        ///   A partition with the specified unique identifier does not exist.
        /// </exception>
        public async Task<Partition> RetrievePartitionAsync(Guid id, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(id, nameof(id));

            await using NpgsqlCommand command = this._dataSource.CreateCommand(PostgresEngineDataStoreImplementationResources.QueryRetrievePartitionById);

            string sid = id.ToString("d");

            command.Parameters.AddWithValue("@id", NpgsqlDbType.Char, 36, sid);

            Partition result = null;

            await using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
            {
                bool exists = await reader.ReadAsync(cancellation);

                if (exists == false)
                {
                    this.RaiseErrorUnknownPartitionIdentifier();
                }

                result = this.ReadPartition(reader);
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

            await using (NpgsqlConnection connection = await this._dataSource.OpenConnectionAsync(cancellation))
            {
                await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellation);

                await using NpgsqlCommand command = new NpgsqlCommand
                {
                    CommandText = PostgresEngineDataStoreImplementationResources.QueryTryAcquirePartition,
                    CommandType = CommandType.Text,
                    Connection = connection,
                    Transaction = transaction
                };

                DateTime uktsTimestamp = DateTime.SpecifyKind(timestamp, DateTimeKind.Unspecified);
                DateTime uktsActive = DateTime.SpecifyKind(active, DateTimeKind.Unspecified);

                command.Parameters.AddWithValue("@job_id", NpgsqlDbType.Char, COLUMN_JOB_NAME_LENGTH, jobId);
                command.Parameters.AddWithValue("@owner", NpgsqlDbType.Varchar, COLUMN_PARTITION_OWNER_LENGTH, requester);
                command.Parameters.AddWithValue("@updated", NpgsqlDbType.Timestamp, uktsTimestamp);
                command.Parameters.AddWithValue("@active", NpgsqlDbType.Timestamp, uktsActive);

                await using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
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

            await using (NpgsqlConnection connection = await this._dataSource.OpenConnectionAsync(cancellation))
            {
                await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellation);

                await using NpgsqlCommand command = new NpgsqlCommand
                {
                    CommandText = PostgresEngineDataStoreImplementationResources.QueryTryRequestSplit,
                    CommandType = CommandType.Text,
                    Connection = connection,
                    Transaction = transaction
                };

                DateTime uktsActive = DateTime.SpecifyKind(active, DateTimeKind.Unspecified);

                command.Parameters.AddWithValue("@job_id", NpgsqlDbType.Char, COLUMN_JOB_NAME_LENGTH, jobId);
                command.Parameters.AddWithValue("@active", NpgsqlDbType.Timestamp, uktsActive);

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

            return result;
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

            await using NpgsqlCommand command = this._dataSource.CreateCommand(PostgresEngineDataStoreImplementationResources.QueryMarkJobAsReady);

            DateTime uktsTimestamp = DateTime.SpecifyKind(timestamp, DateTimeKind.Unspecified);

            command.Parameters.AddWithValue("@id", NpgsqlDbType.Varchar, COLUMN_JOB_NAME_LENGTH, id);
            command.Parameters.AddWithValue("@timestamp", NpgsqlDbType.Timestamp, uktsTimestamp);
            command.Parameters.AddWithValue("@state", JobState.Ready);

            Job result = null;

            await using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
            {
                bool found = await reader.ReadAsync(cancellation);

                if (found == false)
                {
                    this.RaiseErrorUnknownJobIdentifier();
                }

                result = this.ReadJob(reader);
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

            await using NpgsqlCommand command = this._dataSource.CreateCommand(PostgresEngineDataStoreImplementationResources.QueryMarkJobAsStarted);

            DateTime uktsTimestamp = DateTime.SpecifyKind(timestamp, DateTimeKind.Unspecified);

            command.Parameters.AddWithValue("@id", NpgsqlDbType.Varchar, COLUMN_JOB_NAME_LENGTH, id);
            command.Parameters.AddWithValue("@timestamp", NpgsqlDbType.Timestamp, uktsTimestamp);
            command.Parameters.AddWithValue("@state", JobState.Processing);

            Job result = null;

            await using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
            {
                bool found = await reader.ReadAsync(cancellation);

                if (found == false)
                {
                    this.RaiseErrorUnknownJobIdentifier();
                }

                result = this.ReadJob(reader);
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

            await using NpgsqlCommand command = this._dataSource.CreateCommand(PostgresEngineDataStoreImplementationResources.QueryMarkJobAsCompleted);

            DateTime uktsTimestamp = DateTime.SpecifyKind(timestamp, DateTimeKind.Unspecified);

            command.Parameters.AddWithValue("@id", NpgsqlDbType.Varchar, COLUMN_JOB_NAME_LENGTH, id);
            command.Parameters.AddWithValue("@timestamp", NpgsqlDbType.Timestamp, uktsTimestamp);
            command.Parameters.AddWithValue("@state", JobState.Completed);

            Job result = null;

            await using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
            {
                bool found = await reader.ReadAsync(cancellation);

                if (found == false)
                {
                    this.RaiseErrorUnknownJobIdentifier();
                }

                result = this.ReadJob(reader);
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
        public async Task<Job> MarkJobAsFailedAsync(string id, DateTime timestamp, string error, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(id, nameof(id));

            await using NpgsqlCommand command = this._dataSource.CreateCommand(PostgresEngineDataStoreImplementationResources.QueryMarkJobAsFailed);

            DateTime uktsTimestamp = DateTime.SpecifyKind(timestamp, DateTimeKind.Unspecified);

            command.Parameters.AddWithValue("@id", NpgsqlDbType.Varchar, COLUMN_JOB_NAME_LENGTH, id);
            command.Parameters.AddWithValue("@timestamp", NpgsqlDbType.Timestamp, uktsTimestamp);
            command.Parameters.AddWithValue("@state", JobState.Failed);
            command.Parameters.AddWithValue("@error", NpgsqlDbType.Text, ((object) error) ?? DBNull.Value);

            Job result = null;

            await using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
            {
                bool found = await reader.ReadAsync(cancellation);

                if (found == false)
                {
                    this.RaiseErrorUnknownJobIdentifier();
                }

                result = this.ReadJob(reader);
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

            await using NpgsqlCommand command = this._dataSource.CreateCommand(PostgresEngineDataStoreImplementationResources.QueryRetrieveJobStatusReport);

            DateTime uktsTimestamp = DateTime.SpecifyKind(timestamp, DateTimeKind.Unspecified);

            command.Parameters.AddWithValue("@id", NpgsqlDbType.Varchar, COLUMN_JOB_NAME_LENGTH, id);
            command.Parameters.AddWithValue("@timestamp", NpgsqlDbType.Timestamp, uktsTimestamp);

            StatusReport result = null;

            await using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
            {
                bool found = await reader.ReadAsync(cancellation);

                if (found == false)
                {
                    this.RaiseErrorUnknownJobIdentifier();
                }

                Job job = this.ReadJob(reader);

                result = new StatusReport
                {
                    Id = job.Id,
                    Timestamp = uktsTimestamp,
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
        private async Task<bool> DoesJobExistAsync(NpgsqlTransaction transaction, string id, CancellationToken cancellation)
        {
            Debug.Assert(id != null);

            await using NpgsqlCommand command = new NpgsqlCommand
            {
                CommandText = PostgresEngineDataStoreImplementationResources.QueryDoesJobExist,
                CommandType = CommandType.Text,
                Connection = transaction.Connection,
                Transaction = transaction
            };

            command.Parameters.AddWithValue("@id", NpgsqlDbType.Char, COLUMN_JOB_NAME_LENGTH, id);

            long count = (long) await command.ExecuteScalarAsync(cancellation);

            return (count > 0);
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
        private async Task<bool> DoesPartitionExistAsync(NpgsqlTransaction transaction, Guid id, CancellationToken cancellation)
        {
            await using NpgsqlCommand command = new NpgsqlCommand
            {
                CommandText = PostgresEngineDataStoreImplementationResources.QueryDoesPartitionExist,
                CommandType = CommandType.Text,
                Connection = transaction.Connection,
                Transaction = transaction
            };

            string sid = id.ToString("d");

            command.Parameters.AddWithValue("@id", NpgsqlDbType.Char, 36, sid);

            long count = (long) await command.ExecuteScalarAsync(cancellation);

            return (count > 0);
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
            DateTime? started = reader.GetNullableDateTime("started");
            DateTime? completed = reader.GetNullableDateTime("completed");
            JobState state = (JobState) reader.GetValue("state");
            int batchSize = reader.GetInt32("batch_size");
            long ticksTimeout = reader.GetInt64("timeout");
            long ticksClockDrift = reader.GetInt64("clock_drift");
            string error = reader.GetNullableString("error");

            created = DateTime.SpecifyKind(created, DateTimeKind.Utc);
            updated = DateTime.SpecifyKind(updated, DateTimeKind.Utc);
            started = (started != null) ? DateTime.SpecifyKind(started.Value, DateTimeKind.Utc) : null;
            completed = (completed != null) ? DateTime.SpecifyKind(completed.Value, DateTimeKind.Utc) : null;

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

            created = DateTime.SpecifyKind(created, DateTimeKind.Utc);
            updated = DateTime.SpecifyKind(updated, DateTimeKind.Utc);

            Partition result = new Partition(id, jobId, created, updated, first, last, inclusive, position, processed, remaining, owner, completed, throughput, split);

            return result;
        }
    }
}
