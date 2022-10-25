
namespace EXBP.Dipren.Telemetry
{
    /// <summary>
    ///   Holds information about an event that occurred.
    /// </summary>
    public record Event
    {
        /// <summary>
        ///   Gets the date and time of the current event.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value containing the date and time of the current event.
        /// </value>
        public DateTime Timestamp { get; init; }

        /// <summary>
        ///   Gets the component in which the current event occurred.
        /// </summary>
        /// <value>
        ///   An <see cref="EventSource"/> value indicating the component in which the current event occurred.
        /// </value>
        public EventSource Source { get; init; }

        /// <summary>
        ///   Gets the severity of the current event.
        /// </summary>
        /// <value>
        ///   A <see cref="EventSeverity"/> value indicating the severity of the current event.
        /// </value>
        public EventSeverity Severity { get; init; }

        /// <summary>
        ///   Gets the unique identifier of the processing engine instance in which the current event occurred.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value containing the unique identifier of the processing engine instance in which
        ///   the event occurred; or <see langword="null"/> if not available.
        /// </value>
        public string EngineId { get; init; }

        /// <summary>
        ///   Gets the unique identifier of the processing job the current event is related to.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value containing the unique identifier of the processing job the current event is
        ///   related to; or <see langword="null"/> if not available.
        /// </value>
        public string JobId { get; init; }

        /// <summary>
        ///   Gets the unique identifier of the partition the current event is related to.
        /// </summary>
        /// <value>
        ///   A nullable <see cref="Guid"/> value containing the unique identifier of the partition the current event
        ///   is related to; or <see langword="null"/> if not available.
        /// </value>
        public Guid? PartitionId { get; init; }

        /// <summary>
        ///   Gets a description of the current event.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value containing a description of the current event.
        /// </value>
        public string Description { get; init; }

        /// <summary>
        ///   Gets the exception that was thrown.
        /// </summary>
        /// <value>
        ///   A <see cref="Exception"/> object that is the exception that was throw; or <see langword="null"/> if no
        ///   exception was thrown.
        /// </value>
        public Exception Exception { get; init; }


        /// <summary>
        ///   Initializes a new instance of the <see cref="Event"/> record.
        /// </summary>
        internal Event()
        {
        }
    }
}
