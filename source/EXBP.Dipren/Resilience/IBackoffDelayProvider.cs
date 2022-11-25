
namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Allows a type to implement a provider that returns the time to wait before a retry attempt is made.
    /// </summary>
    public interface IBackoffDelayProvider
    {
        /// <summary>
        ///   Returns the time to wait before a retry attempt is made.
        /// </summary>
        /// <param name="attempt">
        ///   The number of the retry attempt.
        /// </param>
        /// <returns>
        ///   A <see cref="TimeSpan"/> value that contains the time to wait before the next retry attempt is made.
        /// </returns>
        TimeSpan GetDelay(int attempt);
    }
}
