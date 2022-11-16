
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   Represents a persisted <see cref="Job{TKey, TItem}"/> object.
    /// </summary>
    public record Job
    {
        private readonly JobState _state;


        /// <summary>
        ///   Gets the unique identifier (or name) of the current distributed processing job.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that is the unique identifier (or name) of the current distributed
        ///   processing job.
        /// </value>
        public string Id { get; }

        /// <summary>
        ///   Gets the date and time when the current job was created.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value that contains the date and time, in UTC, the current job was created.
        /// </value>
        public DateTime Created { get; }

        /// <summary>
        ///   Gets the date and time when the current job was last updated.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value that contains the date and time, in UTC, the current job was last updated.
        /// </value>
        public DateTime Updated { get; init; }

        /// <summary>
        ///   Gets the maximum number of keys to include in a batch.
        /// </summary>
        /// <value>
        ///   A <see cref="int"/> value that contains the maximum number of items to include in a batch.
        /// </value>
        public int BatchSize { get; }

        /// <summary>
        ///   Gets the amount of time after which the processing of a partition is considered unsuccessful or stalled.
        /// </summary>
        /// <value>
        ///   A <see cref="TimeSpan"/> value that specifies the amount of time after which the processing of a
        ///   partition is considered unsuccessful or stalled.
        /// </value>
        public TimeSpan Timeout { get; }

        /// <summary>
        ///   Gets the date and time the current job was started.
        /// </summary>
        /// <value>
        ///   A <see cref="Nullable{T}"/> of <see cref="DateTime"/> value containing the date and time, in UTC, the
        ///   current job was started.
        /// </value>
        public DateTime? Started { get; init; }

        /// <summary>
        ///   Gets the date and time the current job was completed.
        /// </summary>
        /// <value>
        ///   A <see cref="Nullable{T}"/> of <see cref="DateTime"/> value containing the date and time, in UTC, the
        ///   current job was completed.
        /// </value>
        public DateTime? Completed { get; init; }

        /// <summary>
        ///   Gets a value indicating the state of the current job.
        /// </summary>
        /// <value>
        ///   A <see cref="JobState"/> value indicating the state of the current job.
        /// </value>
        public JobState State
        {
            get
            {
                return this._state;
            }
            init
            {
                Assert.ArgumentIsDefined(value, nameof(value));

                this._state = value;
            }
        }

        /// <summary>
        ///   Gets the description of the error that is the reason the current job failed.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains a description of the error that occurred; or
        ///   <see langword="null"/> if no error occurred.
        /// </value>
        public string Error { get; init; }


        /// <summary>
        ///   Initializes a new instance of the <see cref="Job"/> record.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier (or name) of the job.
        /// </param>
        /// <param name="created">
        ///   The date and time when the job was created.
        /// </param>
        /// <param name="updated">
        ///   The date and time when the job was last updated.
        /// </param>
        /// <param name="state">
        ///   The state of the job.
        /// </param>
        /// <param name="batchSize">
        ///   The maximum number of keys to include in a batch.
        /// </param>
        /// <param name="timeout">
        ///   The amount of time after which the processing of a partition is considered unsuccessful or stalled.
        /// </param>
        /// <param name="started">
        ///   The date and time the current job was started.
        /// </param>
        /// <param name="completed">
        ///   The date and time the current job was completed.
        /// </param>
        /// <param name="error">
        ///   The description of the error that caused the job to fail; or <see langword="null"/> if not available.
        /// </param>
        public Job(string id, DateTime created, DateTime updated, JobState state, int batchSize, TimeSpan timeout, DateTime? started = null, DateTime? completed = null, string error = null)
        {
            Assert.ArgumentIsNotNull(id, nameof(id));
            Assert.ArgumentIsGreater(batchSize, 0, nameof(batchSize));
            Assert.ArgumentIsGreater(timeout, TimeSpan.Zero, nameof(timeout));

            this.Id = id;
            this.Created = created;
            this.Updated = updated;
            this.BatchSize = batchSize;
            this.Timeout = timeout;
            this.Started = started;
            this.Completed = completed;
            this.State = state;
            this.Error = error;
        }
    }
}
