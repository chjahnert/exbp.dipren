
namespace EXBP.Dipren.Diagnostics
{
    /// <summary>
    ///   Indicates the severity of a log message.
    /// </summary>
    public enum Severity
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
        ///   The log message provides details about a error condition.
        /// </summary>
        Error
    }
}
