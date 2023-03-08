
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Telemetry
{
    /// <summary>
    ///   Implements an <see cref="IEventHandler"/> that sends log messages to multiple event handlers.
    /// </summary>
    public class CompositeEventHandler : TextEventLogger, IEventHandler
    {
        private readonly IEventHandler[] _handlers;


        /// <summary>
        ///   Initializes a new instance of the <see cref="CompositeEventHandler"/> class.
        /// </summary>
        /// <param name="handlers">
        ///   An <see cref="IEnumerable{T}"/> of <see cref="IEventHandler"/> objects to which to forward the event
        ///   notifications.
        /// </param>
        public CompositeEventHandler(IEnumerable<IEventHandler> handlers)
        {
            Assert.ArgumentIsNotNull(handlers, nameof(handlers));

            this._handlers = handlers.Where(h => (h != null)).ToArray();
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CompositeEventHandler"/> class.
        /// </summary>
        /// <param name="handlers">
        ///   One or more <see cref="IEventHandler"/> objects to which to forward the event notifications.
        /// </param>
        public CompositeEventHandler(params IEventHandler[] handlers)
        {
            Assert.ArgumentIsNotNull(handlers, nameof(handlers));

            this._handlers = handlers.Where(h => (h != null)).ToArray();
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

            for (int i = 0; i < this._handlers.Length; i++)
            {
                this._handlers[i].HandleEvent(descriptor);
            }
        }
    }
}
