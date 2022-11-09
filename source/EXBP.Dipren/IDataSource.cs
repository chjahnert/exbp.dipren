
namespace EXBP.Dipren
{
    /// <summary>
    ///   Allows a class to implement a data source for a distributed processing job. It provides access to the set
    ///   of items to be processed among other operations related to key ranges.
    /// </summary>
    /// <typeparam name="TKey">
    ///   The type of the item key.
    /// </typeparam>
    /// <typeparam name="TItem">
    ///   The type of items to process.
    /// </typeparam>
    public interface IDataSource<TKey, TItem>
    {
        /// <summary>
        ///   Returns the range of keys to process.
        /// </summary>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Range{TKey}"/> that represents the asynchronous operation and
        ///   can be used to access the result.
        /// </returns>
        Task<Range<TKey>> GetEntireRangeAsync(CancellationToken cancellation);

        /// <summary>
        ///   Returns the estimated number of keys in the specified range.
        /// </summary>
        /// <param name="range">
        ///   The key range to estimate.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="long"/> that represents the asynchronous operation and
        ///   can be used to access the result.
        /// </returns>
        Task<long> EstimateRangeSizeAsync(Range<TKey> range, CancellationToken cancellation);

        /// <summary>
        ///   Returns the next batch of items.
        /// </summary>
        /// <param name="range">
        ///   The range of keys from which to retrieve the next batch of items.
        /// </param>
        /// <param name="skip">
        ///   The number of items to skip.
        /// </param>
        /// <param name="take">
        ///   The maximum number of items to return.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="IEnumerable{T}"/> that represents the asynchronous operation
        ///   and can be used to access the result.
        /// </returns>
        Task<IEnumerable<KeyValuePair<TKey, TItem>>> GetNextBatchAsync(Range<TKey> range, int skip, int take, CancellationToken cancellation);
    }
}
