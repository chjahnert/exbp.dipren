
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   Represents a persisted <see cref="Partition{TKey}"/> object.
    /// </summary>
    public record Partition
    {
        private readonly string _last;
        private readonly long _processed;
        private readonly long _remaining;


        /// <summary>
        ///   Gets the unique identifier of the current partition.
        /// </summary>
        /// <value>
        ///   A <see cref="Guid"/> value that is the unique identifier of the current partition.
        /// </value>
        public Guid Id { get; }

        /// <summary>
        ///   Gets the unique identifier of the distributed processing job the current partition belongs to.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains the unique identifier of the distributed processing job the
        ///   current partition belongs to.
        /// </value>
        public string JobId { get; }

        /// <summary>
        ///   Gets a value that identifies the owner of the current partition. The owner is the node processing the
        ///   items in the partition.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that uniquely identifies the node that processes the items in the partition;
        ///   or <see langword="null"/> if the partition is not associated with a processing node.
        /// </value>
        public string Owner { get; init; }

        /// <summary>
        ///   Gets the date and time when the current partition was created.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value that contains the date and time, in UTC, the current partition was
        ///   created.
        /// </value>
        public DateTime Created { get; }

        /// <summary>
        ///   Gets the date and time when the current partition was last updated.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value that contains the date and time, in UTC, the current partition was
        ///   last updated.
        /// </value>
        public DateTime Updated { get; init; }

        /// <summary>
        ///   Gets the first key of the current range.
        /// </summary>
        /// <value>
        ///   A <see cref="TKey"/> value that is the first key in the range.
        /// </value>
        /// <remarks>
        ///   The key does not have to actually exist.
        /// </remarks>
        public string First { get; }

        /// <summary>
        ///   Gets the key at which to start processing.
        /// </summary>
        /// <value>
        ///   A <see cref="TKey"/> value that is the key at which to start processing.
        /// </value>
        /// <remarks>
        ///   The key does not have to actually exist.
        /// </remarks>
        public string Last
        {
            get
            {
                return this._last;
            }
            init
            {
                Assert.ArgumentIsNotNull(value, nameof(value));

                this._last = value;
            }
        }

        /// <summary>
        ///   Gets a value indicating whether the current range is including <see cref="Last"/>.
        /// </summary>
        /// <value>
        ///   <see langword="true"/> if <see cref="Last"/> is included in the current range; otherwise,
        ///   <see langword="false"/>.
        /// </value>
        public bool IsInclusive { get; init; }

        /// <summary>
        ///   Gets the key of the last item that was processed.
        /// </summary>
        /// <value>
        ///   A <typeparamref name="TKey"/> value that contains the  key last of the last item that was processed.
        /// </value>
        /// <remarks>
        ///   This value is only set after at least one item was processed. Check <see cref="Processed"/> to determine
        ///   whether this value is set.
        /// </remarks>
        public string Position { get; init; }

        /// <summary>
        ///   Gets the number of items processed in the current partition.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value that contains the number of items processed in the current partition.
        /// </value>
        public long Processed
        {
            get
            {
                return this._processed;
            }
            init
            {
                Assert.ArgumentIsGreaterOrEqual(value, 0L, nameof(value));

                this._processed = value;
            }
        }

        /// <summary>
        ///   Gets the estimated number of unprocessed items in the current partition.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value that contains the estimated number of unprocessed items in the current
        ///   partition.
        /// </value>
        /// <remarks>
        ///   This value indicates the throughput of the processing node owning the current partition. It maybe the
        ///   last observed value or the moving average of the last couple of batches.
        /// </remarks>
        public long Remaining
        {
            get
            {
                return this._remaining;
            }
            init
            {
                Assert.ArgumentIsGreaterOrEqual(value, 0L, nameof(value));

                this._remaining = value;
            }
        }

        /// <summary>
        ///   Gets a value indicating whether the current partition has been processed.
        /// </summary>
        /// <value>
        ///  <see langword="true"/> if the partition is completed; otherwise, <see langword="false"/>.
        /// </value>
        public bool IsCompleted { get; init; }

        /// <summary>
        ///   Gets the estimated number of items processed per second.
        /// </summary>
        /// <value>
        ///   A <see cref="double"/> value that contains the estimated number of items processed per second.
        /// </value>
        public double Throughput { get; init; }

        /// <summary>
        ///   Gets a value indicating whether a split was requested.
        /// </summary>
        /// <value>
        ///   <see langword="true"/> if a split was requested; otherwise, <see langword="false"/>.
        /// </value>
        public bool IsSplitRequested { get; init; }


        /// <summary>
        ///   Initializes a new instance of the <see cref="Partition"/> record.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the partition.
        /// </param>
        /// <param name="jobId">
        ///   The unique identifier of the distributed processing job.
        /// </param>
        /// <param name="created">
        ///   The date and time when the partition was created.
        /// </param>
        /// <param name="updated">
        ///   The date and time when the partition was last updated.
        /// </param>
        /// <param name="first">
        ///   The string representation of the first key in the partition's key range.
        /// </param>
        /// <param name="last">
        ///   The string representation of the last key in the partition's key range.
        /// </param>
        /// <param name="inclusive">
        ///   A <see cref="bool"/> value indicating whether <paramref name="last"/> is included in the key range.
        /// </param>
        /// <param name="position">
        ///   The string representation of the key of the last item that was processed in the partition or
        ///   <see langword="null"/> if no item was process yet.
        /// </param>
        /// <param name="processed">
        ///   The number of items processed in the partition so far.
        /// </param>
        /// <param name="remaining">
        ///   The estimated number of unprocessed items in the partition.
        /// </param>
        /// <param name="owner">
        ///   The owner of the partition or <see langword="null"/>.
        /// </param>
        /// <param name="completed">
        ///   <see langword="true"/> if the partition is completed; otherwise, <see langword="false"/>.
        /// </param>
        /// <param name="throughput">
        ///   The number of items processed per second.
        /// </param>
        /// <param name="split">
        ///   <see langword="true"/> if a split is requested; otherwise, <see langword="false"/>.
        /// </param>
        public Partition(Guid id, string jobId, DateTime created, DateTime updated, string first, string last, bool inclusive, string position, long processed, long remaining, string owner = null, bool completed = false, double throughput = 0.0, bool split = false)
        {
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));
            Assert.ArgumentIsNotNull(first, nameof(first));

            this.Id = id;
            this.JobId = jobId;
            this.Created = created;
            this.First = first;
            this.Owner = owner;
            this.Updated = updated;
            this.Last = last;
            this.IsInclusive = inclusive;
            this.Position = position;
            this.Processed = processed;
            this.Remaining = remaining;
            this.IsCompleted = completed;
            this.Throughput = throughput;
            this.IsSplitRequested = split;
        }
    }
}
