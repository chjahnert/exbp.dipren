
namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Allows a type to implement a retry strategy for asynchronous operations.
    /// </summary>
    public interface IAsyncRetryStrategy
    {
        /// <summary>
        ///   Executes the specified action with the current retry strategy.
        /// </summary>
        /// <param name="action">
        ///   The action to execute.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> object that represents the asynchronous operation.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="action"/> is a <see langword="null"/> reference.
        /// </exception>
        Task ExecuteAsync(Func<Task> action, CancellationToken cancellation = default);

        /// <summary>
        ///   Executes the specified action with the current retry strategy.
        /// </summary>
        /// <typeparam name="TResult">
        ///   The type of the result returned by the asynchronous operation.
        /// </typeparam>
        /// <param name="action">
        ///   The action to execute.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> object that represents the asynchronous operation.
        /// </returns>
        Task<TResult> ExecuteAsync<TResult>(Func<Task<TResult>> action, CancellationToken cancellation = default);
    }
}
