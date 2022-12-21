
namespace EXBP.Dipren
{
    /// <summary>
    ///   Allows a class to implement a key range partitioner.
    /// </summary>
    /// <typeparam name="TKey">
    ///   The type of keys.
    /// </typeparam>
    public interface IRangePartitioner<TKey>
    {
        /// <summary>
        ///   Splits the specified range into two ranges.
        /// </summary>
        /// <param name="range">
        ///   The <see cref="Range{TKey}"/> to split.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="RangePartitioningResult{TKey}"/> object that holds the results of the operation.
        /// </returns>
        Task<RangePartitioningResult<TKey>> SplitAsync(Range<TKey> range, CancellationToken cancellation);
    }
}
