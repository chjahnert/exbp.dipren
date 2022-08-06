
using System.Diagnostics;

using EXBP.Dipren.Diagnostics;

namespace EXBP.Dipren
{
    /// <summary>
    ///   Represents a partition of the data being processed.
    /// </summary>
    /// <typeparam name="TKey">
    ///   The type of the keys.
    /// </typeparam>
    internal class Partition<TKey> where TKey : IComparable<TKey>
    {
        private readonly Guid _id;
        private readonly Guid _jobId;
        private readonly string _owner;
        private readonly DateTime _created;
        private readonly DateTime _updated;
        private readonly Range<TKey> _range;
        private readonly TKey _position;
        private readonly long _processed;
        private readonly long _remaining;


        /// <summary>
        ///   Gets the unique identifier of the current partition.
        /// </summary>
        /// <value>
        ///   A <see cref="Guid"/> value that is the unique identifier of the current partition.
        /// </value>
        public Guid Id =>  this._id;

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
        ///   Gets the key range for the current partition.
        /// </summary>
        /// <value>
        ///   A <see cref="Range{TKey}"/> object that defines the key range for the current partition.
        /// </value>
        public Range<TKey> Range => this._range;

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
        public TKey Position => this._position;

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
        ///   Initializes a new instance of the <see cref="Partition{TKey}"/> class.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the partition.
        /// </param>
        /// <param name="jobId">
        ///   The unique identifier of the distributed processing job.
        /// </param>
        /// <param name="owner">
        ///   Identifies the owner of the partition
        /// </param>
        /// <param name="created">
        ///   The date and time when the partition was created, expressed as UTC time.
        /// </param>
        /// <param name="updated">
        ///   The date and time when the partition was last updated, expressed as UTC time.
        /// </param>
        /// <param name="range">
        ///   The key range for the partition.
        /// </param>
        /// <param name="position">
        ///   The key of the last item that was processed.
        /// </param>
        /// <param name="processed">
        ///   The number of items processed.
        /// </param>
        /// <param name="remaining">
        ///   The estimated number of unprocessed items.
        /// </param>
        internal Partition(Guid id, Guid jobId, string owner, DateTime created, DateTime updated, Range<TKey> range, TKey position, long processed, long remaining)
        {
            Debug.Assert(created.Kind == DateTimeKind.Utc);
            Debug.Assert(updated.Kind == DateTimeKind.Utc);
            Debug.Assert(range != null);
            Debug.Assert(processed >= 0L);
            Debug.Assert(remaining >= 0L);

            this._id = id;
            this._jobId = jobId;
            this._owner = owner;
            this._created = created;
            this._updated = updated;
            this._range = range;
            this._position = position;
            this._processed = processed;
            this._remaining = remaining;
        }
    }
}
