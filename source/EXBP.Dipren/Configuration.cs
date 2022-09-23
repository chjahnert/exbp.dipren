
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
        private const double DEFAULT_BATCH_PROCESSING_TIMEOUT_SECONDS = 60.0;


        private readonly TimeSpan _maximumClockDrift = TimeSpan.FromSeconds(DEFAULT_MAXIMUM_CLOCK_DRIFT_SECONDS);
        private readonly TimeSpan _pollingInterval = TimeSpan.FromSeconds(DEFAULT_POLLING_INTERVAL_SECONDS);
        private readonly TimeSpan _batchProcessingTimeout = TimeSpan.FromSeconds(DEFAULT_BATCH_PROCESSING_TIMEOUT_SECONDS);


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
        ///   Gets the amount of time after which the processing of a batch is considered unsuccessful.
        /// </summary>
        /// <value>
        ///   A <see cref="TimeSpan"/> value that indicates the amount of time after which the processing of a batch
        ///   is considered unsuccessful.
        /// </value>
        /// <remarks>
        ///   <para>
        ///     When processing a batch takes longer than the specified timeout values, the operation is not canceled
        ///     automatically. But if another processing node tries to acquire a partition and the partition was not
        ///     updated for more than this timeout value, the partition is considered abandoned and might be taken by
        ///     another processing node.
        ///   </para>
        /// </remarks>
        public TimeSpan BatchProcessingTimeout
        {
            get
            {
                return this._batchProcessingTimeout;
            }
            init
            {
                this._batchProcessingTimeout = (value >= TimeSpan.Zero) ? value : (-1 * value);
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
        ///   The maximum system time difference between processing nodes.
        /// </param>
        /// <param name="pollingInterval">
        ///   The interval at which changes are read from the engine data store.
        /// </param>
        /// <param name="batchProcessingTimeout">
        ///   The amount of time after which the processing of a batch is considered unsuccessful.
        /// </param>
        public Configuration(TimeSpan maximumClockDrift, TimeSpan pollingInterval, TimeSpan batchProcessingTimeout)
        {
            this.MaximumClockDrift = maximumClockDrift;
            this.PollingInterval = pollingInterval;
            this.BatchProcessingTimeout = batchProcessingTimeout;
        }
    }
}
