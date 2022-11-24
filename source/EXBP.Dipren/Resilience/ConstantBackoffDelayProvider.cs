
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Implements a <see cref="IBackoffDelayProvider"/> that returns a constant waiting time for each retry attempt.
    /// </summary>
    public class ConstantBackoffDelayProvider : IBackoffDelayProvider
    {
        private readonly TimeSpan _delay;


        /// <summary>
        ///   Initializes a new instance of the <see cref="ConstantBackoffDelayProvider"/> class.
        /// </summary>
        /// <param name="delay">
        ///   The time to wait before each retry attempt.
        /// </param>
        /// <exception cref="ArgumentException">
        ///   Argument <paramref name="delay"/> is a negative value.
        /// </exception>
        public ConstantBackoffDelayProvider(TimeSpan delay)
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
            => this._delay;
    }
}
