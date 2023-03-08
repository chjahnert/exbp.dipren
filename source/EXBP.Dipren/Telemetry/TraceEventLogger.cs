
using System.Diagnostics;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Telemetry
{
    /// <summary>
    ///   Implements an <see cref="IEventHandler"/> that sends log messages about events to the trace output.
    /// </summary>
    public class TraceEventLogger : TextEventLogger, IEventHandler
    {
        private readonly EventSeverity _level;


        /// <summary>
        ///   Gets the default instance of the <see cref="TraceEventLogger"/> type that outputs all messages.
        /// </summary>
        /// <value>
        ///   A <see cref="TraceEventLogger"/> instance that outputs all messages.
        /// </value>
        public static TraceEventLogger Debug { get; } = new TraceEventLogger(EventSeverity.Debug);

        /// <summary>
        ///   Gets the default instance of the <see cref="TraceEventLogger"/> type that outputs information and error
        ///   messages.
        /// </summary>
        /// <value>
        ///   A <see cref="TraceEventLogger"/> instance that outputs information and error messages.
        /// </value>
        public static TraceEventLogger Information { get; } = new TraceEventLogger(EventSeverity.Information);

        /// <summary>
        ///   Gets the default instance of the <see cref="TraceEventLogger"/> type that outputs error messages only.
        /// </summary>
        /// <value>
        ///   A <see cref="TraceEventLogger"/> instance that outputs error messages only.
        /// </value>
        public static TraceEventLogger Error { get; } = new TraceEventLogger(EventSeverity.Error);


        /// <summary>
        ///   Initializes a new instance of the <see cref="TraceEventLogger"/> class.
        /// </summary>
        /// <param name="level">
        ///   The minimum severity level of the messages to output.
        /// </param>
        protected TraceEventLogger(EventSeverity level)
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
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> that represents the asynchronous operation.
        /// </returns>
        public virtual Task HandleEventAsync(EventDescriptor descriptor, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(descriptor, nameof(descriptor));

            if (descriptor.Severity >= this._level)
            {
                string output = this.Format(descriptor);

                Trace.WriteLine(output);
            }

            return Task.CompletedTask;
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

                Trace.WriteLine(output);
            }
        }
    }
}
