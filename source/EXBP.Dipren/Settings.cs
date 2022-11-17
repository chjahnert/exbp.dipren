
using System.Diagnostics;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Holds the settings for a distributed processing job.
    /// </summary>
    [DebuggerDisplay("BatchSize = {BatchSize}, Timeout = {Timeout}, MaximumClockDrift = {MaximumClockDrift}")]
    public class Settings
    {
        private const int DEFAULT_MAXIMUM_CLOCK_DRIFT_MS = 2000;


        /// <summary>
        ///   Gets the default value for the <see cref="MaximumClockDrift"/> property.
        /// </summary>
        /// <value>
        ///   A <see cref="TimeSpan"/> value that contains the default value of the <see cref="MaximumClockDrift"/>
        ///   property.
        /// </value>
        internal static TimeSpan DefaultMaximumClockDrift => TimeSpan.FromMilliseconds(DEFAULT_MAXIMUM_CLOCK_DRIFT_MS);


        /// <summary>
        ///   Gets the maximum number of items to include in a batch.
        /// </summary>
        /// <value>
        ///   A <see cref="int"/> value that contains the maximum number of items to include in a batch.
        /// </value>
        public int BatchSize { get; }

        /// <summary>
        ///   Gets the amount of time after which the processing of a partition is considered unsuccessful or stalled.
        /// </summary>
        /// <value>
        ///   A <see cref="TimeSpan"/> value that specifies the amount of time after which the processing of a
        ///   partition is considered unsuccessful or stalled.
        /// </value>
        /// <remarks>
        ///   <para>
        ///     When processing a batch takes longer than the specified timeout value, the operation is not canceled
        ///     automatically, but if another processing node tries to acquire a partition and a partition was not
        ///     updated for more than this timeout value, the partition is considered abandoned and might be taken over
        ///     by the other processing node.
        ///   </para>
        ///   <para>
        ///     Consider the time it takes to process a single batch of items and the time it takes to call the
        ///     <see cref="IDataSource{TKey, TItem}.EstimateRangeSizeAsync(Range{TKey}, CancellationToken)"/> method
        ///     for the entire key range. The timeout should be greater than it takes to perform either of these
        ///     operations.
        ///   </para>
        /// </remarks>
        public TimeSpan Timeout { get; }


        /// <summary>
        ///   Gets the maximum time divergence between processing nodes.
        /// </summary>
        /// <value>
        ///   A <see cref="TimeSpan"/> value that contains the maximum time divergence between processing nodes. The
        ///   default value is 2 seconds.
        /// </value>
        public TimeSpan MaximumClockDrift { get; }


        /// <summary>
        ///   Initializes a new instance of the <see cref="Settings"/> class.
        /// </summary>
        /// <param name="batchSize">
        ///   The maximum number of keys to include in a batch.
        /// </param>
        /// <param name="timeout">
        ///   The amount of time after which the processing of a partition is considered unsuccessful or stalled.
        /// </param>
        /// <param name="maximumClockDrift">
        ///   The maximum time divergence between processing nodes.
        /// </param>
        public Settings(int batchSize, TimeSpan timeout, TimeSpan maximumClockDrift)
        {
            Assert.ArgumentIsGreater(batchSize, 0, nameof(batchSize));
            Assert.ArgumentIsGreater(timeout, TimeSpan.Zero, nameof(timeout));

            this.BatchSize = batchSize;
            this.Timeout = timeout;
            this.MaximumClockDrift = (maximumClockDrift >= TimeSpan.Zero) ? maximumClockDrift : (-1 * maximumClockDrift);
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="Settings"/> class.
        /// </summary>
        /// <param name="batchSize">
        ///   The maximum number of keys to include in a batch.
        /// </param>
        /// <param name="timeout">
        ///   The amount of time after which the processing of a partition is considered unsuccessful or stalled.
        /// </param>
        public Settings(int batchSize, TimeSpan timeout) : this(batchSize, timeout, Settings.DefaultMaximumClockDrift)
        {
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="Settings"/> class.
        /// </summary>
        /// <param name="batchSize">
        ///   The maximum number of keys to include in a batch.
        /// </param>
        /// <param name="timeout">
        ///   The amount of time, expressed in milliseconds, after which the processing of a partition is considered
        ///   unsuccessful or stalled.
        /// </param>
        /// <param name="maximumClockDrift">
        ///   The maximum time divergence between processing nodes expressed in milliseconds.
        /// </param>
        public Settings(int batchSize, int timeout, int maximumClockDrift = DEFAULT_MAXIMUM_CLOCK_DRIFT_MS) : this(batchSize, TimeSpan.FromMilliseconds(timeout), TimeSpan.FromMilliseconds(maximumClockDrift))
        {
        }
    }
}
