
using System.Diagnostics;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Holds the key ranges returned by the partitioning function.
    /// </summary>
    /// <typeparam name="TKey">
    ///   The type of the key.
    /// </typeparam>
    [DebuggerDisplay("Success = {Success}, Count = {Created.Count}")]
    public class RangePartitioningResult<TKey>
    {
        /// <summary>
        ///   Gets the updated version of the key range that was split.
        /// </summary>
        /// <value>
        ///   A <see cref="Range{TKey}"/> of <typeparamref name="TKey"/> object that holds the updated version of the
        ///   key range that was split.
        /// </value>
        public Range<TKey> Updated { get; }

        /// <summary>
        ///   Gets the collection of new key ranges created.
        /// </summary>
        /// <value>
        ///   An <see cref="IReadOnlyCollection{T}"/> of <see cref="Range{TKey}"/> object that contains the key ranges
        ///   created.
        /// </value>
        public IReadOnlyCollection<Range<TKey>> Created { get; }

        /// <summary>
        ///   Gets a value indicating whether the partitioning operation was successful.
        /// </summary>
        /// <value>
        ///   <see langword="true"/> if the partitioning was successful and the <see cref="Created"/> property contains
        ///   one or more new partitions; otherwise <see langword="false"/>.
        /// </value>
        public bool Success => (this.Created.Count > 0);


        /// <summary>
        ///   Initializes a new instance of the <see cref="RangePartitioningResult{TKey}"/> class.
        /// </summary>
        /// <param name="updated">
        ///   The updated version of the key range that was split.
        /// </param>
        /// <param name="created">
        ///   The collection of new key ranges created.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="created"/> or argument <paramref name="updated"/> is a null reference.
        /// </exception>
        public RangePartitioningResult(Range<TKey> updated, IEnumerable<Range<TKey>> created)
        {
            Assert.ArgumentIsNotNull(updated, nameof(updated));
            Assert.ArgumentIsNotNull(created, nameof(created));

            this.Created = new List<Range<TKey>>(created);
            this.Updated = updated;
        }   
    }
}
