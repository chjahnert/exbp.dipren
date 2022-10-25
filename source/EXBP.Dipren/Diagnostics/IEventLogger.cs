
namespace EXBP.Dipren.Diagnostics
{
    /// <summary>
    ///   Allows a class to implement a logger that receives notifications about events during processing.
    /// </summary>
    public interface IEventLogger
    {
        /// <summary>
        ///   Logs an event.
        /// </summary>
        /// <param name="engineId">
        ///   The unique identifier of the engine in which the event occurred.
        /// </param>
        /// <param name="jobId">
        ///   The unique identifier of the distributed processing job the log event is related to; or
        ///   <see langword="null"/> if not available.
        /// </param>
        /// <param name="partitionId">
        ///   The unique identifier of the partition the log event is related to; or <see langword="null"/> if not
        ///   available.
        /// </param>
        /// <param name="severity">
        ///   A <see cref="Severity"/> value indicating the severity of the event.
        /// </param>
        /// <param name="message">
        ///   A description of the event.
        /// </param>
        /// <param name="exception">
        ///   The exception describing the error condition; or <see langword="null"/> if not available.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> that represents the asynchronous operation.
        /// </returns>
        Task LogAsync(string engineId, string jobId, Guid? partitionId, Severity severity, string message, Exception exception = null);
    }
}
