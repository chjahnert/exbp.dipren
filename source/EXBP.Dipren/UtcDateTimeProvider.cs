
namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements an <see cref="IDateTimeProvider"/> that returns the current date and time, expressed as UTC time.
    /// </summary>
    internal class UtcDateTimeProvider : IDateTimeProvider
    {
        /// <summary>
        ///   Gets a default instance of the <see cref="UtcDateTimeProvider"/> class.
        /// </summary>
        /// <value>
        ///   A <see cref="UtcDateTimeProvider"/> instance that is the default instance of this class.
        /// </value>
        public static UtcDateTimeProvider Default { get; } = new UtcDateTimeProvider();


        /// <summary>
        ///   Initializes a new instance of the <see cref="UtcDateTimeProvider"/> class.
        /// </summary>
        protected UtcDateTimeProvider( )
        {
        }

        /// <summary>
        ///   Gets the current date and time, expressed as UTC time.
        /// </summary>
        /// <returns>
        ///   A <see cref="DateTime"/> value that contains the current date and time, expressed as UTC time.
        /// </returns>
        public DateTime GetDateTime( )
        {
            DateTime current = DateTime.UtcNow;
            DateTime result = this.OnFormatValue( current );

            return result;
        }

        /// <summary>
        ///   Called before the current date and time value is returned.
        /// </summary>
        /// <param name="value">
        ///   The current date time value returned by the operating system.
        /// </param>
        /// <returns>
        ///   The value to return to the consumer of the API.
        /// </returns>
        protected virtual DateTime OnFormatValue(DateTime value)
        {
            return value;
        }
    }
}

