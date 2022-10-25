
namespace EXBP.Dipren.Telemetry
{
    /// <summary>
    ///   Allows a class to implement a logger that receives notifications about events during processing.
    /// </summary>
    public interface IEventLogger
    {
        /// <summary>
        ///   Logs an event.
        /// </summary>
        /// <param name="event">
        ///   An <see cref="Event"/> object that holds information about the event that occurred.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> that represents the asynchronous operation.
        /// </returns>
        Task LogAsync(Event @event, CancellationToken cancellation);
    }
}
