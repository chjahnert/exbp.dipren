
namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements an <see cref="ITimestampProvider"/> that returns the current timestamp as a <see cref="DateTime"/>
    ///   value, expressed as UTC time.
    /// </summary>
    internal class UtcTimestampProvider : ITimestampProvider
    {
        /// <summary>
        ///   Gets a default instance of the <see cref="UtcTimestampProvider"/> class.
        /// </summary>
        /// <value>
        ///   A <see cref="UtcTimestampProvider"/> instance that is the default instance of this class.
        /// </value>
        public static UtcTimestampProvider Default { get; } = new UtcTimestampProvider();


        /// <summary>
        ///   Initializes a new instance of the <see cref="UtcTimestampProvider"/> class.
        /// </summary>
        protected UtcTimestampProvider( )
        {
        }

        /// <summary>
        ///   Gets the current timestamp.
        /// </summary>
        /// <returns>
        ///   A <see cref="DateTime"/> value that contains the current timestamp, expressed as UTC time.
        /// </returns>
        public DateTime GetCurrentTimestamp( )
        {
            DateTime current = DateTime.UtcNow;
            DateTime result = this.OnFormatValue( current );

            return result;
        }

        /// <summary>
        ///   Called before the current timestamp value is returned.
        /// </summary>
        /// <param name="value">
        ///   The current date time value returned by the operating system.
        /// </param>
        /// <returns>
        ///   The value to return to the consumer of the API.
        /// </returns>
        /// <remarks>
        ///   This method can be used, for example, to adjust the precision of the returned value.
        /// </remarks>
        protected virtual DateTime OnFormatValue(DateTime value)
        {
            return value;
        }
    }
}

