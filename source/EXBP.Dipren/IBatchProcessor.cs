
namespace EXBP.Dipren
{
    /// <summary>
    ///   Allows a class to implement a type that processes a batch of items.
    /// </summary>
    /// <typeparam name="TItem">
    ///   The type of the item.
    /// </typeparam>
    public interface IBatchProcessor<TItem>
    {
        /// <summary>
        ///   Processes a batch of items.
        /// </summary>
        /// <param name="items">
        ///   The batch of items to process.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> that represents the asynchronous operation.
        /// </returns>
        Task ProcessAsync(IEnumerable<TItem> items, CancellationToken cancellation);
    }
}
