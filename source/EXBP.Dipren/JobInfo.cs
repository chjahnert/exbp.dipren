
using EXBP.Dipren.Diagnostics;

namespace EXBP.Dipren
{
    /// <summary>
    ///   Provides information about a distributed processing job.
    /// </summary>
    public class JobInfo
    {
        /// <summary>
        ///   Gets the unique identifier (or name) of the current distributed processing job.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains the unique identifier of the current distributed processing
        ///   job.
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
        public DateTime Updated { get; }

        /// <summary>
        ///   Gets the last observed state of the current job.
        /// </summary>
        /// <value>
        ///   A <see cref="JobState"/> value containing the last observed state of the job.
        /// </value>
        public JobState State { get; }

        /// <summary>
        ///   Gets the description of the error that is the reason the current job failed.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains a description of the error that occurred; or
        ///   <see langword="null"/> if no error occurred.
        /// </value>
        public string Error { get; }

        /// <summary>
        ///   Gets the number of partitions not yet started.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value containing the number of partitions not yet started.
        /// </value>
        public long PartitionsWaiting { get; }

        /// <summary>
        ///   Gets the number of partially completed partitions.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value containing the number of partially completed partitions.
        /// </value>
        public long PartitionsStarted { get; }

        /// <summary>
        ///   Gets the number of completed partitions.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value containing the number of completed partitions.
        /// </value>
        public long PartitionsCompleted { get; }

        /// <summary>
        ///   Gets the number of keys left to process.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value containing the number of keys left to process.
        /// </value>
        public long KeysRemaining { get; }

        /// <summary>
        ///   Gets the number of keys completed.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value containing the number of keys completed.
        /// </value>
        public long KeysCompleted { get; }

        /// <summary>
        ///   Gets the date and time of the last activity observed on the job or any partition.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value that contains the date and time, in UTC, of the last activity observed on
        ///   the job or any partition.
        /// </value>
        public DateTime LastActivity { get; }

        /// <summary>
        ///   Gets a number indicating how often a processing node resumed a partition started by another processing
        ///   node.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value containing a number indicating how often a processing node resumed a partition
        ///   started by another processing node.
        /// </value>
        public long OwnershipChanges { get; }

        /// <summary>
        ///   Gets the number of pending split requests.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value containing the number of active split requests.
        /// </value>
        public long PendingSplitRequests { get; }

        /// <summary>
        ///   Gets the throughput of the current distributed processing job expressed in keys/second.
        /// </summary>
        /// <value>
        ///   A <see cref="double"/> value containing the throughput of the current distributed processing job
        ///   expressed in keys/second.
        /// </value>
        public double KeysPerSecond { get; }

        /// <summary>
        ///   Gets the estimated time remaining until the current distributed processing job completes.
        /// </summary>
        /// <value>
        ///   A <see cref="TimeSpan"/> value containing the estimated time remaining until the current distributed
        ///   processing job completes.
        /// </value>
        public TimeSpan EstimatedTimeToCompletion { get; }

        /// <summary>
        ///   Initializes a new instance of the <see cref="JobInfo"/> class.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier (or name) of the distributed processing job.
        /// </param>
        /// <param name="created">
        ///   The date and time when the job was created.
        /// </param>
        /// <param name="updated">
        ///   The date and time when the job was last updated.
        /// </param>
        /// <param name="state">
        ///   The last observed state of the job.
        /// </param>
        /// <param name="error">
        ///   The description of the error that is the reason the job failed; or <see langword="null"/> if no error
        ///   occurred.
        /// </param>
        /// <param name="partitionsWaiting">
        ///   The number of partitions not yet started.
        /// </param>
        /// <param name="partitionsStarted">
        ///   The number of partially completed partitions.
        /// </param>
        /// <param name="partitionsCompleted">
        ///   The number of completed partitions.
        /// </param>
        /// <param name="keysRemaining">
        ///   The number of keys left to process.
        /// </param>
        /// <param name="keysCompleted">
        ///   The number of keys completed.
        /// </param>
        /// <param name="lastActivity">
        ///   The date and time of the last activity observed on the job or any partition.
        /// </param>
        /// <param name="ownershipChanges">
        ///   A number indicating how often a processing node resumed a partition started by another processing node.
        /// </param>
        /// <param name="pendingSplitRequests">
        ///   The number of pending split requests.
        /// </param>
        public JobInfo(string id, DateTime created, DateTime updated, JobState state, string error, long partitionsWaiting, long partitionsStarted, long partitionsCompleted, long keysRemaining, long keysCompleted, DateTime lastActivity, long ownershipChanges, long pendingSplitRequests)
        {
            Assert.ArgumentIsNotNull(id, nameof(id));
            Assert.ArgumentIsDefined(state, nameof(state));
            Assert.ArgumentIsGreaterOrEqual(partitionsWaiting, 0L, nameof(partitionsWaiting));
            Assert.ArgumentIsGreaterOrEqual(partitionsStarted, 0L, nameof(partitionsStarted));
            Assert.ArgumentIsGreaterOrEqual(partitionsCompleted, 0L, nameof(partitionsCompleted));
            Assert.ArgumentIsGreaterOrEqual(keysRemaining, 0L, nameof(keysRemaining));
            Assert.ArgumentIsGreaterOrEqual(keysCompleted, 0L, nameof(keysCompleted));
            Assert.ArgumentIsGreaterOrEqual(ownershipChanges, 0L, nameof(ownershipChanges));
            Assert.ArgumentIsGreaterOrEqual(pendingSplitRequests, 0L, nameof(pendingSplitRequests));

            this.Id = id;
            this.Created = created;
            this.Updated = updated;
            this.State = state;
            this.Error = error;
            this.PartitionsWaiting = partitionsWaiting;
            this.PartitionsStarted = partitionsStarted;
            this.PartitionsCompleted = partitionsCompleted;
            this.KeysRemaining = keysRemaining;
            this.KeysCompleted = keysCompleted;
            this.LastActivity = lastActivity;
            this.OwnershipChanges = ownershipChanges;
            this.PendingSplitRequests = pendingSplitRequests;

            //
            // Calculated field.
            // Need to add columns 'started' and 'completed'.
            //

            this.KeysPerSecond = 0;
            this.EstimatedTimeToCompletion = TimeSpan.Zero;
        }   
    }
}
