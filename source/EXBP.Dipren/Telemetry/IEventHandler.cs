
namespace EXBP.Dipren.Telemetry
{
    /// <summary>
    ///   Allows a class to implement an event handler that receives notifications about events during processing.
    /// </summary>
    public interface IEventHandler
    {
        /// <summary>
        ///   Handles the event.
        /// </summary>
        /// <param name="descriptor">
        ///   An <see cref="EventDescriptor"/> object that holds information about the event that occurred.
        /// </param>
        void HandleEvent(EventDescriptor descriptor);
    }
}
