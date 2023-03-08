
using System.Text;


namespace EXBP.Dipren.Diagnostics
{
    /// <summary>
    ///   Serves as abase class for event handlers that output events into a text stream.
    /// </summary>
    public abstract class TextEventLogger
    {
        /// <summary>
        ///   Initializes a new instance of the <see cref="TextEventLogger"/> class.
        /// </summary>
        protected TextEventLogger()
        {
        }

        /// <summary>
        ///   Returns a string representation of the specified event.
        /// </summary>
        /// <param name="descriptor">
        ///   The event to return the string representation for.
        /// </param>
        /// <returns>
        ///   A <see cref="string"/> value that can be output to a text stream.
        /// </returns>
        protected string Format(EventDescriptor descriptor)
        {
            Assert.ArgumentIsNotNull(descriptor, nameof(descriptor));

            StringBuilder builder = new StringBuilder();

            builder.AppendFormat("{0:yyyy-MM-dd HH:mm:ss.ffff}", descriptor.Timestamp);

            builder.Append(" [");
            builder.Append(descriptor.EngineId);
            builder.Append(", ");
            builder.Append(descriptor.JobId);

            if (descriptor.PartitionId != null)
            {
                builder.Append(", ");
                builder.AppendFormat("{0:D}", descriptor.PartitionId.Value);
            }

            builder.Append("]");

            builder.Append(" ");

            switch (descriptor.Severity)
            {
                case EventSeverity.Debug:
                    builder.Append(TextEventLoggerResources.Debug);
                    break;

                case EventSeverity.Information:
                    builder.Append(TextEventLoggerResources.Information);
                    break;

                case EventSeverity.Error:
                    builder.Append(TextEventLoggerResources.Error);
                    break;
            }

            builder.Append(": ");
            builder.Append(descriptor.Description);

            if (descriptor.Exception != null)
            {
                builder.AppendLine();
                builder.Append(descriptor.Exception);
            }

            string result = builder.ToString();

            return result;
        }
    }
}
