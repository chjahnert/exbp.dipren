
using System.Diagnostics;
using System.Text;


namespace EXBP.Dipren.Diagnostics
{
    /// <summary>
    ///   Implements an <see cref="IEventLogger"/> that sends log messages to the trace output.
    /// </summary>
    public class TraceEventLogger : IEventLogger
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
        public virtual Task LogAsync(Event @event, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(@event, nameof(@event));

            if (@event.Severity >= this._level)
            {
                StringBuilder builder = new StringBuilder();

                builder.Append(TraceEventLoggerResources.Dipren);

                if ((@event.EngineId != null) || (@event.JobId != null) || (@event.PartitionId != null))
                {
                    builder.Append(" [");

                    bool first = true;

                    if (@event.EngineId != null)
                    {
                        builder.Append(@event.EngineId);

                        first = false;
                    }

                    if (@event.JobId != null)
                    {
                        if (first == false)
                        {
                            builder.Append("|");
                        }

                        builder.Append(@event.JobId);

                        if (@event.PartitionId != null)
                        {
                            builder.Append("|");
                            builder.AppendFormat("{0}:D", @event.PartitionId.Value);
                        }
                    }

                    builder.Append("]");
                }

                builder.Append(" ");

                switch (@event.Severity)
                {
                    case EventSeverity.Debug:
                        builder.Append(TraceEventLoggerResources.Debug);
                        break;

                    case EventSeverity.Information:
                        builder.Append(TraceEventLoggerResources.Information);
                        break;

                    case EventSeverity.Error:
                        builder.Append(TraceEventLoggerResources.Error);
                        break;
                }

                builder.Append(": ");
                builder.Append(@event.Description);

                if (@event.Exception != null)
                {
                    builder.AppendLine();
                    builder.Append(@event.Exception);
                }

                string output = builder.ToString();

                Trace.WriteLine(output);
            }

            return Task.CompletedTask;
        }
    }
}
