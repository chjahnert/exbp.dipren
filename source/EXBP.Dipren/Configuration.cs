
namespace EXBP.Dipren
{
    /// <summary>
    ///   Holds the configuration settings to be used by an <see cref="Engine"/> instance.
    /// </summary>
    public class Configuration
    {
        private const double DEFAULT_MAXIMUM_CLOCK_DRIFT = 2.0;


        private readonly TimeSpan _maximumClockDrift = TimeSpan.FromSeconds(DEFAULT_MAXIMUM_CLOCK_DRIFT);


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
                return _maximumClockDrift;
            }
            init
            {
                this._maximumClockDrift = (value >= TimeSpan.Zero) ? value : (-1 * value);
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
        public Configuration(TimeSpan maximumClockDrift)
        {
            this.MaximumClockDrift = maximumClockDrift;
        }
    }
}
