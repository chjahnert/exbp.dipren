
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Resilience
{
    /// <summary>
    ///   Implements a <see cref="IBackoffDelayProvider"/> that returns a preset waiting time for each retry attempt.
    /// </summary>
    public class PresetBackoffDelayProvider : IBackoffDelayProvider
    {
        private readonly TimeSpan[] _delay;


        /// <summary>
        ///   Initializes a new instance of the <see cref="PresetBackoffDelayProvider"/> class.
        /// </summary>
        /// <param name="delay">
        ///   The time to wait before each retry attempt.
        /// </param>
        /// <exception cref="ArgumentException">
        ///   Argument <paramref name="delay"/> is a negative value.
        /// </exception>
        public PresetBackoffDelayProvider(TimeSpan delay) : this(new TimeSpan[] { delay })
        {
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="PresetBackoffDelayProvider"/> class.
        /// </summary>
        /// <param name="delays">
        ///   An array of <see cref="TimeSpan"/> values containing the delays to return in order.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="delays"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///   Argument <paramref name="delays"/> is an empty array.
        /// </exception>
        public PresetBackoffDelayProvider(TimeSpan[] delays)
        {
            Assert.ArgumentIsNotNull(delays, nameof(delays));
            Assert.ArgumentIsNotEmpty(delays, nameof(delays));

            this._delay = delays;
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
        {
            int index = (attempt - 1);

            if (index < 0)
            {
                index = 0;
            }
            else
            {
                if (index >= _delay.Length)
                {
                    index = (this._delay.Length - 1);
                }
            }

            TimeSpan result = _delay[index];

            return result;
        }
    }
}
