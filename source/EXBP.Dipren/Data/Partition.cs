
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   Represents a persisted <see cref="Partition{TKey}"/> object.
    /// </summary>
    public record Partition
    {
        private readonly Guid _id;
        private readonly Guid _jobId;
        private readonly string _owner;
        private readonly DateTime _created;
        private readonly DateTime _updated;
        private readonly string _first;
        private readonly string _last;
        private readonly bool _inclusive;
        private readonly string _position;
        private readonly long _processed;
        private readonly long _remaining;
        private readonly bool _split;


        /// <summary>
        ///   Gets the unique identifier of the current partition.
        /// </summary>
        /// <value>
        ///   A <see cref="Guid"/> value that is the unique identifier of the current partition.
        /// </value>
        public Guid Id => this._id;

        /// <summary>
        ///   Gets the unique identifier of the distributed processing job the current partition belongs to.
        /// </summary>
        /// <value>
        ///   A <see cref="Guid"/> value that contains the unique identifier of the distributed processing job the
        ///   current partition belongs to.
        /// </value>
        public Guid JobId => this._jobId;

        /// <summary>
        ///   Gets a value that identifies the owner of the current partition. The owner is the node processing the
        ///   items in the partition.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that uniquely identifies the node that processes the items in the partition;
        ///   or <see langword="null"/> if the partition is not associated with a processing node.
        /// </value>
        public string Owner => this._owner;

        /// <summary>
        ///   Gets the date and time when the current partition was created.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value that contains the date and time, in UTC, the current partition was
        ///   created.
        /// </value>
        public DateTime Created => this._created;

        /// <summary>
        ///   Gets the date and time when the current partition was last updated.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value that contains the date and time, in UTC, the current partition was
        ///   last updated.
        /// </value>
        public DateTime Updated => this._updated;

        /// <summary>
        ///   Gets the first key of the current range.
        /// </summary>
        /// <value>
        ///   A <see cref="TKey"/> value that is the first key in the range.
        /// </value>
        /// <remarks>
        ///   The key does not have to actually exist.
        /// </remarks>
        public string First => this._first;

        /// <summary>
        ///   Gets the key at which to start processing.
        /// </summary>
        /// <value>
        ///   A <see cref="TKey"/> value that is the key at which to start processing.
        /// </value>
        /// <remarks>
        ///   The key does not have to actually exist.
        /// </remarks>
        public string Last => this._last;

        /// <summary>
        ///   Gets a value indicating whether the current range is including <see cref="Last"/>.
        /// </summary>
        /// <value>
        ///   <see langword="true"/> if <see cref="Last"/> is included in the current range; otherwise,
        ///   <see langword="false"/>.
        /// </value>
        public bool IsInclusive => this._inclusive;

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
        public string Position => this._position;

        /// <summary>
        ///   Gets the number of items processed in the current partition.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value that contains the number of items processed in the current partition.
        /// </value>
        public long Processed => this._processed;

        /// <summary>
        ///   Gets the estimated number of unprocessed items in the current partition.
        /// </summary>
        /// <value>
        ///   A <see cref="long"/> value that contains the estimated number of unprocessed items in the current
        ///   partition.
        /// </value>
        public long Remaining => this._remaining;

        /// <summary>
        ///   Gets a value indicating whether a split was requested.
        /// </summary>
        /// <value>
        ///   <see langword="true"/> if a split was requested; otherwise, <see langword="false"/>.
        /// </value>
        public bool IsSplitRequested => this._split;


        /// <summary>
        ///   Initializes a new instance of the <see cref="Partition"/> record.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the partition.
        /// </param>
        /// <param name="jobId">
        ///   The unique identifier of the distributed processing job.
        /// </param>
        /// <param name="owner">
        ///   The owner of the partition.
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
        ///   The string representation of the key of the last item that was processed in the partition.
        /// </param>
        /// <param name="processed">
        ///   The number of items processed in the partition so far.
        /// </param>
        /// <param name="remaining">
        ///   The estimated number of unprocessed items in the partition.
        /// </param>
        /// <param name="split">
        ///   <see langword="true"/> if a split is requested; otherwise, <see langword="false"/>.
        /// </param>
        public Partition(Guid id, Guid jobId, string owner, DateTime created, DateTime updated, string first, string last, bool inclusive, string position, long processed, long remaining, bool split = false)
        {
            Assert.ArgumentIsNotNull(owner, nameof(owner));
            Assert.ArgumentIsNotNull(first, nameof(first));
            Assert.ArgumentIsNotNull(last, nameof(last));
            Assert.ArgumentIsNotNull(position, nameof(position));
            Assert.ArgumentIsGreaterOrEqual(processed, 0L, nameof(processed));
            Assert.ArgumentIsGreaterOrEqual(remaining, 0L, nameof(remaining));

            this._id = id;
            this._jobId = jobId;
            this._owner = owner;
            this._created = created;
            this._updated = updated;
            this._first = first;
            this._last = last;
            this._inclusive = inclusive;
            this._position = position;
            this._processed = processed;
            this._remaining = remaining;
            this._split = split;
        }
    }
}
