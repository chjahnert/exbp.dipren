
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Implements a <see cref="IBackoffDelayProvider"/> that doubles the waiting time before each subsequent retry
    ///   attempt.
    /// </summary>
    /// <remarks>
    ///   <para>
    ///     See <see href="https://en.wikipedia.org/wiki/Exponential_backoff"/>Exponential Backoff</see> for details.
    ///   </para>
    /// </remarks>
    public class ExponentialBackoffDelayProvider : IBackoffDelayProvider
    {
        private readonly TimeSpan _delay;

        /// <summary>
        ///   Initializes a new instance of the <see cref="ExponentialBackoffDelayProvider"/> class.
        /// </summary>
        /// <param name="delay">
        ///   The time to wait at the first retry attempt.
        /// </param>
        /// <exception cref="ArgumentException">
        ///   Argument <paramref name="delay"/> is a negative value.
        /// </exception>
        public ExponentialBackoffDelayProvider(TimeSpan delay)
        {
            Assert.ArgumentIsGreaterOrEqual(delay, TimeSpan.Zero, nameof(delay));

            this._delay = delay;
        }


        /// <summary>
        ///   Returns the time to wait before a retry attempt is made.
        /// </summary>
        /// <param name="attempt">
        ///   The number of the retry attempt.
        /// </param>
        /// <returns>
        ///   A <see cref="TimeSpan"/> value that contains the time to wait before the next retry attempt is made.
        /// </returns>
        public TimeSpan GetDelay(int attempt)
            => (this._delay * Math.Pow(2, (attempt - 1)));
    }
}
