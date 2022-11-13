
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
        ///   Gets a value indicating the state of the current job.
        /// </summary>
        /// <value>
        ///   A <see cref="JobState"/> value indicating the state of the current job.
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
        ///   Gets the number of partitions that were created, but were not yet picked up by a processing node.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value containing the number of partitions that were created, but were not yet picked
        ///   up by a processing node.
        /// </value>
        public long PartitionsWaiting { get; }

        /// <summary>
        ///   Gets the number of partitions picked up by a processing node, but are not yet complete.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value containing the number of partitions picked up by a processing node, but are
        ///   not yet complete.
        /// </value>
        public long PartitionsStarted { get; }

        /// <summary>
        ///   Gets the number of partitions completed.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value containing the number of partitions completed.
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
        ///   Gets the date and time of the last activity observed on any partition.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value that contains the date and time, in UTC, of the last activity observed on
        ///   any partition.
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
        /// <param name="id"></param>
        /// <param name="created"></param>
        /// <param name="updated"></param>
        /// <param name="state"></param>
        /// <param name="error"></param>
        /// <param name="partitionsWaiting"></param>
        /// <param name="partitionsStarted"></param>
        /// <param name="partitionsCompleted"></param>
        /// <param name="keysRemaining"></param>
        /// <param name="keysCompleted"></param>
        /// <param name="lastActivity"></param>
        /// <param name="ownershipChanges"></param>
        /// <param name="pendingSplitRequests"></param>
        public JobInfo(string id, DateTime created, DateTime updated, JobState state, string error, long partitionsWaiting, long partitionsStarted, long partitionsCompleted, long keysRemaining, long keysCompleted, DateTime lastActivity, long ownershipChanges, long pendingSplitRequests)
        {
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
