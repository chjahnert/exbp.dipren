
using System.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Holds the configuration settings to be used by an <see cref="Engine"/> instance.
    /// </summary>
    [DebuggerDisplay("Maximum Clock Drift = {MaximumClockDrift}, Polling Interval = {PollingInterval}")]
    public class Configuration
    {
        private const double DEFAULT_MAXIMUM_CLOCK_DRIFT_SECONDS = 2.0;
        private const double DEFAULT_POLLING_INTERVAL_SECONDS = 2.0;


        private readonly TimeSpan _maximumClockDrift = TimeSpan.FromSeconds(DEFAULT_MAXIMUM_CLOCK_DRIFT_SECONDS);
        private readonly TimeSpan _pollingInterval = TimeSpan.FromSeconds(DEFAULT_POLLING_INTERVAL_SECONDS);


        /// <summary>
        ///   Gets the maximum time divergence between processing nodes.
        /// </summary>
        /// <value>
        ///   A <see cref="TimeSpan"/> value that contains the maximum time divergence between processing nodes. The
        ///   default value is 2 seconds.
        /// </value>
        public TimeSpan MaximumClockDrift
        {
            get
            {
                return this._maximumClockDrift;
            }
            init
            {
                this._maximumClockDrift = (value >= TimeSpan.Zero) ? value : (-1 * value);
            }
        }

        /// <summary>
        ///   Gets the polling interval for changes in the engine data store.
        /// </summary>
        /// <value>
        ///   A <see cref="TimeSpan"/> value that contains the interval at which changes are read from the engine data
        ///   store. The default value is 2 seconds.
        /// </value>
        public TimeSpan PollingInterval
        {
            get
            {
                return this._pollingInterval;
            }
            init
            {
                this._pollingInterval = (value >= TimeSpan.Zero) ? value : (-1 * value);
            }
        }


        /// <summary>
        ///   Initializes a new instance of the <see cref="Configuration"/> class.
        /// </summary>
        public Configuration()
        {
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="Configuration"/> class.
        /// </summary>
        /// <param name="maximumClockDrift">
        ///   The maximum time divergence between processing nodes.
        /// </param>
        /// <param name="pollingInterval">
        ///   The interval at which changes are read from the engine data store.
        /// </param>
        public Configuration(TimeSpan maximumClockDrift, TimeSpan pollingInterval)
        {
            this.MaximumClockDrift = maximumClockDrift;
            this.PollingInterval = pollingInterval;
        }
    }
}
