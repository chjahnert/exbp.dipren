
using System.Diagnostics;

namespace EXBP.Dipren
{
    /// <summary>
    ///   Holds a summary of details for a distributed processing job.
    /// </summary>
    [DebuggerDisplay("Id = {Id}, State = {State}, Error = {Error}")]
    public class Summary
    {
        /// <summary>
        ///   Gets the unique identifier (or name) of the current distributed processing job.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains the unique identifier of the current distributed processing
        ///   job.
        /// </value>
        public string Id { get; init; }

        /// <summary>
        ///   Gets the date and time when the current job was created.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value that contains the date and time, in UTC, the current job was created.
        /// </value>
        public DateTime Created { get; init; }

        /// <summary>
        ///   Gets the date and time when the current job was last updated.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value that contains the date and time, in UTC, the current job was last updated.
        /// </value>
        public DateTime Updated { get; init; }

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
        ///   Gets the last observed state of the current job.
        /// </summary>
        /// <value>
        ///   A <see cref="JobState"/> value containing the last observed state of the job.
        /// </value>
        public JobState State { get; init; }

        /// <summary>
        ///   Gets the description of the error that is the reason the current job failed.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains a description of the error that occurred; or
        ///   <see langword="null"/> if no error occurred.
        /// </value>
        public string Error { get; init; }

        /// <summary>
        ///   Gets the partition count grouped by their state.
        /// </summary>
        /// <value>
        ///   A <see cref="KeyCounts"/> object containing the number of partitions grouped by their state.
        /// </value>
        public PartitionCounts Partitions { get; init; }

        /// <summary>
        ///   Gets the key count grouped by their state.
        /// </summary>
        /// <value>
        ///   A <see cref="KeyCounts"/> object containing the number of keys grouped by their state.
        /// </value>
        public KeyCounts Keys { get; init; }

        /// <summary>
        ///   Gets the date and time of the last activity observed on the job or any partition.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value that contains the date and time, in UTC, of the last activity observed on
        ///   the job or any partition.
        /// </value>
        public DateTime LastActivity { get; init; }

        /// <summary>
        ///   Gets a number indicating how often a processing node resumed a partition started by another processing
        ///   node.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value containing a number indicating how often a processing node resumed a partition
        ///   started by another processing node.
        /// </value>
        public long? OwnershipChanges { get; init; }

        /// <summary>
        ///   Gets the number of pending split requests.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value containing the number of active split requests.
        /// </value>
        public long? PendingSplitRequests { get; init; }

        /// <summary>
        ///   Holds the number of partitions grouped by their state.
        /// </summary>
        public class PartitionCounts
        {
            /// <summary>
            ///   Gets the number of partitions that are ready for processing.
            /// </summary>
            /// <value>
            ///   A <see cref="long"/> value containing the number of partitions ready for processing.
            /// </value>
            public long Untouched { get; init; }

            /// <summary>
            ///   Gets the number of partially completed partitions.
            /// </summary>
            /// <value>
            ///   A <see cref="long"/> value containing the number of partially completed partitions.
            /// </value>
            public long InProgress { get; init; }

            /// <summary>
            ///   Gets the number of completed partitions.
            /// </summary>
            /// <value>
            ///   A <see cref="long"/> value containing the number of completed partitions.
            /// </value>
            public long Completed { get; init; }

            /// <summary>
            ///   Gets the total number of partitions created. 
            /// </summary>
            /// <value>
            ///   An <see cref="Int64"/> value containing the total number of partitions.
            /// </value>
            public long Total => (this.Untouched + this.InProgress + this.Completed);
        }

        /// <summary>
        ///   Holds the number of keys by their state.
        /// </summary>
        public class KeyCounts
        {
            /// <summary>
            ///   Gets the estimated number of keys left to process.
            /// </summary>
            /// <value>
            ///   A <see cref="long"/> value containing the estimated number of keys left to process.
            /// </value>
            public long? Remaining { get; init; }

            /// <summary>
            ///   Gets the number of keys completed.
            /// </summary>
            /// <value>
            ///   A <see cref="long"/> value containing the number of keys completed.
            /// </value>
            public long? Completed { get; init; }

            /// <summary>
            ///   Gets the estimated number of keys.
            /// </summary>
            /// <value>
            ///   A <see cref="long"/> value containing the estimated number of keys.
            /// </value>
            public long? Total => (this.Completed + this.Remaining);
        }
    }
}
