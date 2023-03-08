
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Diagnostics
{
    /// <summary>
    ///   Implements an <see cref="IEventHandler"/> that sends log messages about events to the standard output.
    /// </summary>
    public class ConsoleEventLogger : TextEventLogger, IEventHandler
    {
        private readonly EventSeverity _level;


        /// <summary>
        ///   Gets the default instance of the <see cref="ConsoleEventLogger"/> type that outputs all messages.
        /// </summary>
        /// <value>
        ///   A <see cref="ConsoleEventLogger"/> instance that outputs all messages.
        /// </value>
        public static ConsoleEventLogger Debug { get; } = new ConsoleEventLogger(EventSeverity.Debug);

        /// <summary>
        ///   Gets the default instance of the <see cref="ConsoleEventLogger"/> type that outputs information and error
        ///   messages.
        /// </summary>
        /// <value>
        ///   A <see cref="ConsoleEventLogger"/> instance that outputs information and error messages.
        /// </value>
        public static ConsoleEventLogger Information { get; } = new ConsoleEventLogger(EventSeverity.Information);

        /// <summary>
        ///   Gets the default instance of the <see cref="ConsoleEventLogger"/> type that outputs error messages only.
        /// </summary>
        /// <value>
        ///   A <see cref="ConsoleEventLogger"/> instance that outputs error messages only.
        /// </value>
        public static ConsoleEventLogger Error { get; } = new ConsoleEventLogger(EventSeverity.Error);


        /// <summary>
        ///   Initializes a new instance of the <see cref="ConsoleEventLogger"/> class.
        /// </summary>
        /// <param name="level">
        ///   The minimum severity level of the messages to output.
        /// </param>
        protected ConsoleEventLogger(EventSeverity level)
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

                Console.WriteLine(output);
            }
        }
    }
}
