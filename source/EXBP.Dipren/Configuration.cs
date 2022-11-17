
using System.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Holds the configuration settings to be used by an <see cref="Engine"/> instance.
    /// </summary>
    [DebuggerDisplay("Polling Interval = {PollingInterval}")]
    public class Configuration
    {
        private const double DEFAULT_POLLING_INTERVAL_SECONDS = 2.0;


        private readonly TimeSpan _pollingInterval = TimeSpan.FromSeconds(DEFAULT_POLLING_INTERVAL_SECONDS);


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
        /// <param name="pollingInterval">
        ///   The interval at which changes are read from the engine data store.
        /// </param>
        public Configuration(TimeSpan pollingInterval)
        {
            this.PollingInterval = pollingInterval;
        }
    }
}
