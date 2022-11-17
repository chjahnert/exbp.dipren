
namespace EXBP.Dipren.Telemetry
{
    /// <summary>
    ///   Indicates the severity of a log message.
    /// </summary>
    public enum EventSeverity
    {
        /// <summary>
        ///   The log message provides details usually only needed for debugging purposes.
        /// </summary>
        Debug,

        /// <summary>
        ///   The log message provides details about a more important step and operation.
        /// </summary>
        Information,

        /// <summary>
        ///   The log message provides details about a possible error condition.
        /// </summary>
        Warning,

        /// <summary>
        ///   The log message provides details about a error condition.
        /// </summary>
        Error
    }
}
