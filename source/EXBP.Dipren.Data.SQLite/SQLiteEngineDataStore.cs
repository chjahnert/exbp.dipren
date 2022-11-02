
using System.Data;
using System.Data.SQLite;

using EXBP.Dipren.Data.SQLite;
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Data.Memory
{
    /// <summary>
    ///   Implements an <see cref="IEngineDataStore"/> that uses SQLite as its storage engine.
    /// </summary>
    public class SQLiteEngineDataStore : IEngineDataStore
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

            long result = (long) await command.ExecuteScalarAsync(cancellation);

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

            using SQLiteTransaction transaction = this._connection.BeginTransaction();

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.SqlInsertJob,
                CommandType = CommandType.Text,
                Connection = _connection,
                Transaction = transaction
            };

            command.Parameters.AddWithValue("$id", job.Id);
            command.Parameters.AddWithValue("$created", job.Created);
            command.Parameters.AddWithValue("$updated", job.Updated);
            command.Parameters.AddWithValue("$state", job.State);
            command.Parameters.AddWithValue("$exception", job.Exception);

            await command.ExecuteNonQueryAsync(cancellation);

            transaction.Commit();
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

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.SqlInsertPartition,
                CommandType = CommandType.Text,
                Connection = _connection,
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
            command.Parameters.AddWithValue("$inclusive", partition.IsInclusive);
            command.Parameters.AddWithValue("$position", partition.Position);
            command.Parameters.AddWithValue("$processed", partition.Processed);
            command.Parameters.AddWithValue("$remaining", partition.Remaining);
            command.Parameters.AddWithValue("$is_completed", partition.IsCompleted);
            command.Parameters.AddWithValue("$is_split_requested", partition.IsSplitRequested);

            await command.ExecuteNonQueryAsync(cancellation);

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
        public Task InsertSplitPartitionAsync(Partition partitionToUpdate, Partition partitionToInsert, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(partitionToUpdate, nameof(partitionToUpdate));
            Assert.ArgumentIsNotNull(partitionToInsert, nameof(partitionToInsert));

            throw new NotImplementedException();
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
        /// <param name="exception">
        ///   The exception, if available, that provides information about the error.
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
        public Task<Job> UpdateJobAsync(string jobId, DateTime timestamp, JobState state, Exception exception, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));

            throw new NotImplementedException();
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

            throw new NotImplementedException();
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

            throw new NotImplementedException();
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

            /*
            SELECT
              *
            FROM
              "partitions"
            WHERE
              ("job_id" = 'DPJ-0001') AND
              (("owner" IS NULL) OR ("updated" < '2022-11-01 22:00:00')) AND
              ("is_completed" = 0)
            ORDER BY
              "remaining" DESC
            LIMIT
              1;
            */

            throw new NotImplementedException();
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

            throw new NotImplementedException();
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
        public Task<Partition> ReportProgressAsync(Guid id, string owner, DateTime timestamp, string position, long progress, bool completed, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(owner, nameof(owner));
            Assert.ArgumentIsNotNull(position, nameof(position));

            throw new NotImplementedException();
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

            using SQLiteTransaction transaction = connection.BeginTransaction();

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SQLiteEngineDataStoreResources.SqlCreateSchema,
                CommandType = CommandType.Text,
                Connection = connection,
                Transaction = transaction
            };

            await command.ExecuteNonQueryAsync();

            transaction.Commit();

            SQLiteEngineDataStore result = new SQLiteEngineDataStore(connection);

            return result;
        }
    }
}
