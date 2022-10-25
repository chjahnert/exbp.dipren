
using System.Diagnostics;
using System.Text;


namespace EXBP.Dipren.Diagnostics
{
    /// <summary>
    ///   Implements an <see cref="IEventLogger"/> that sends log messages to the trace output.
    /// </summary>
    public class TraceEventLogger : IEventLogger
    {
        private readonly Severity _level;


        /// <summary>
        ///   Gets the default instance of the <see cref="TraceEventLogger"/> type that outputs all messages.
        /// </summary>
        /// <value>
        ///   A <see cref="TraceEventLogger"/> instance that outputs all messages.
        /// </value>
        public static TraceEventLogger Debug { get; } = new TraceEventLogger(Severity.Debug);

        /// <summary>
        ///   Gets the default instance of the <see cref="TraceEventLogger"/> type that outputs information and error
        ///   messages.
        /// </summary>
        /// <value>
        ///   A <see cref="TraceEventLogger"/> instance that outputs information and error messages.
        /// </value>
        public static TraceEventLogger Information { get; } = new TraceEventLogger(Severity.Information);

        /// <summary>
        ///   Gets the default instance of the <see cref="TraceEventLogger"/> type that outputs error messages only.
        /// </summary>
        /// <value>
        ///   A <see cref="TraceEventLogger"/> instance that outputs error messages only.
        /// </value>
        public static TraceEventLogger Error { get; } = new TraceEventLogger(Severity.Error);


        /// <summary>
        ///   Initializes a new instance of the <see cref="TraceEventLogger"/> class.
        /// </summary>
        /// <param name="level">
        ///   The minimum severity level of the messages to output.
        /// </param>
        protected TraceEventLogger(Severity level)
        {
            Assert.ArgumentIsDefined(level, nameof(level));

            this._level = level;
        }


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
        public virtual Task LogAsync(string engineId, string jobId, Guid? partitionId, Severity severity, string message, Exception exception = null)
        {
            if (severity >= this._level)
            {
                StringBuilder builder = new StringBuilder();

                builder.Append(TraceEventLoggerResources.Dipren);
                builder.Append(": [");
                builder.Append(engineId);

                if (jobId != null)
                {
                    builder.Append("|");
                    builder.Append(jobId);

                    if (partitionId != null)
                    {
                        builder.Append("|");
                        builder.AppendFormat("{0}:D", partitionId.Value);
                    }
                }

                builder.Append("] ");

                switch (severity)
                {
                    case Severity.Debug:
                        builder.Append(TraceEventLoggerResources.Debug);
                        break;

                    case Severity.Information:
                        builder.Append(TraceEventLoggerResources.Information);
                        break;

                    case Severity.Error:
                        builder.Append(TraceEventLoggerResources.Error);
                        break;
                }

                builder.Append(": ");
                builder.Append(message);

                if (exception != null)
                {
                    builder.AppendLine();
                    builder.Append(exception);
                }

                string output = builder.ToString();

                Trace.WriteLine(output);
            }

            return Task.CompletedTask;
        }
    }
}
