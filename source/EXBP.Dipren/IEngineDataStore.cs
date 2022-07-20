
namespace EXBP.Dipren
{
    /// <summary>
    ///   Allows a class to implement a data store for the distributed processing engine.
    /// </summary>
    public interface IEngineDataStore
    {
        /// <summary>
        ///   Returns the number of distributed processing jobs in the current data store.
        /// </summary>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> or <see cref="long"/> that represents the asynchronous operation and  can
        ///   be used to access the result.
        /// </returns>
        Task<long> CountJobsAsync(CancellationToken cancellation);
    }
}
