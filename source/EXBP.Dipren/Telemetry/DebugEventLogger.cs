
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Telemetry
{
    /// <summary>
    ///   Implements an <see cref="IEventHandler"/> that sends log messages about events to the debug output.
    /// </summary>
    public class DebugEventLogger : TextEventLogger, IEventHandler
    {
        private readonly EventSeverity _level;


        /// <summary>
        ///   Gets the default instance of the <see cref="DebugEventLogger"/> type that outputs all messages.
        /// </summary>
        /// <value>
        ///   A <see cref="DebugEventLogger"/> instance that outputs all messages.
        /// </value>
        public static DebugEventLogger Debug { get; } = new DebugEventLogger(EventSeverity.Debug);

        /// <summary>
        ///   Gets the default instance of the <see cref="DebugEventLogger"/> type that outputs information and error
        ///   messages.
        /// </summary>
        /// <value>
        ///   A <see cref="DebugEventLogger"/> instance that outputs information and error messages.
        /// </value>
        public static DebugEventLogger Information { get; } = new DebugEventLogger(EventSeverity.Information);

        /// <summary>
        ///   Gets the default instance of the <see cref="DebugEventLogger"/> type that outputs error messages only.
        /// </summary>
        /// <value>
        ///   A <see cref="DebugEventLogger"/> instance that outputs error messages only.
        /// </value>
        public static DebugEventLogger Error { get; } = new DebugEventLogger(EventSeverity.Error);


        /// <summary>
        ///   Initializes a new instance of the <see cref="DebugEventLogger"/> class.
        /// </summary>
        /// <param name="level">
        ///   The minimum severity level of the messages to output.
        /// </param>
        protected DebugEventLogger(EventSeverity level)
        {
            Assert.ArgumentIsDefined(level, nameof(level));

            this._level = level;
        }


        /// <summary>
        ///   Handles events.
        /// </summary>
        /// <param name="descriptor">
        ///   An <see cref="EventDescriptor"/> object that holds information about the event that occurred.
        /// </param>
        public virtual void HandleEvent(EventDescriptor descriptor)
        {
            Assert.ArgumentIsNotNull(descriptor, nameof(descriptor));

            if (descriptor.Severity >= this._level)
            {
                string output = this.Format(descriptor);

                System.Diagnostics.Debug.WriteLine(output);
            }
        }
    }
}
