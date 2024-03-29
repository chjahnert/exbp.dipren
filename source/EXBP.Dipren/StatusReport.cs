﻿
using System.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Represents a status report generated for a distributed processing job.
    /// </summary>
    [DebuggerDisplay("Id = {Id}, State = {State}, Error = {Error}")]
    public class StatusReport
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
        ///   Gets the date and time the current status report was generated.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value containing the date and time the current status report was generated,
        ///   expressed in UTC.
        /// </value>
        public DateTime Timestamp { get; init; }

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
        ///   Gets the maximum number of items that are included in a batch.
        /// </summary>
        /// <value>
        ///   A <see cref="int"/> value that contains the maximum number of items that are included in a batch.
        /// </value>
        public int BatchSize { get; init; }

        /// <summary>
        ///   Gets the amount of time after which the processing of a partition is considered unsuccessful or stalled.
        /// </summary>
        /// <value>
        ///   A <see cref="TimeSpan"/> value that specifies the amount of time after which the processing of a
        ///   partition is considered unsuccessful or stalled.
        /// </value>
        public TimeSpan Timeout { get; init; }

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
        ///   A <see cref="ProgressReport"/> object containing the number of partitions grouped by their state.
        /// </value>
        public PartitionsReport Partitions { get; init; }

        /// <summary>
        ///   Gets details about the progress made.
        /// </summary>
        /// <value>
        ///   A <see cref="ProgressReport"/> object containing the amount of keys processed and remaining.
        /// </value>
        public ProgressReport Progress { get; init; }

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
        ///   Gets the current throughput of all processing nodes working on this job.
        /// </summary>
        /// <value>
        ///   A <see cref="double"/> value that contains the current throughput of all processing nodes working on this
        ///   job. The throughput is expressed as number of items processed per second.
        /// </value>
        public double CurrentThroughput { get; init; }

        /// <summary>
        ///   Holds the number of partitions grouped by their state.
        /// </summary>
        public class PartitionsReport
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
        ///   Provides information about the number of keys processed and remaining.
        /// </summary>
        public class ProgressReport
        {
            /// <summary>
            ///   Gets the estimated size of the dataset (the number of keys) not yet completed.
            /// </summary>
            /// <value>
            ///   A <see cref="long"/> value containing the estimated number of keys not yet completed.
            /// </value>
            public long? Remaining { get; init; }

            /// <summary>
            ///   Gets the size of the dataset (the number of keys) completed.
            /// </summary>
            /// <value>
            ///   A <see cref="long"/> value containing the number of keys completed.
            /// </value>
            public long? Completed { get; init; }

            /// <summary>
            ///   Gets the estimated size of the dataset.
            /// </summary>
            /// <value>
            ///   A <see cref="long"/> value containing the total number of keys estimated.
            /// </value>
            public long? Total => (this.Completed + this.Remaining);

            /// <summary>
            ///   Gets the ratio of completed keys.
            /// </summary>
            /// <value>
            ///   A nullable <see cref="double"/> value containing the ratio of completed keys.
            /// </value>
            public double? Ratio => ((double?) this.Completed / this.Total);
        }
    }
}
