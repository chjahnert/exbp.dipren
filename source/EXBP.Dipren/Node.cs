
using System.Diagnostics;

using EXBP.Dipren.Data;
using EXBP.Dipren.Diagnostics;
using EXBP.Dipren.Telemetry;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements a base class for various node types such as the scheduler and the processing engine.
    /// </summary>
    public abstract class Node
    {
        private readonly string _id;
        private readonly IEngineDataStore _store;
        private readonly IDateTimeProvider _clock;
        private readonly EventDispatcher _dispatcher;


        /// <summary>
        ///   Gets the unique identifier of the current node.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains the unique identifier of the current node.
        /// </value>
        public string Id => this._id;

        /// <summary>
        ///   Gets the data store for the current node.
        /// </summary>
        /// <value>
        ///   A <see cref="IEngineDataStore"/> object that provides access to persisted jobs and partitions.
        /// </value>
        protected IEngineDataStore Store => this._store;

        /// <summary>
        ///   Gets the date and time provider that can be use to generate timestamp values.
        /// </summary>
        /// <value>
        ///   A <see cref="IDateTimeProvider"/> object that can be used to generate timestamp values.
        /// </value>
        protected IDateTimeProvider Clock => this._clock;

        /// <summary>
        ///   Gets the event dispatcher that is used to emit event notifications.
        /// </summary>
        /// <value>
        ///   A <see cref="EventDispatcher"/> object that is used to emit event notification.
        /// </value>
        protected EventDispatcher Dispatcher => this._dispatcher;


        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="type">
        ///   A <see cref="NodeType"/> value that indicates the type of the current node.
        /// </param>
        /// <param name="store">
        ///   The <see cref="IEngineDataStore"/> to use.
        /// </param>
        /// <param name="clock">
        ///   The <see cref="IDateTimeProvider"/> to use to generate timestamps.
        /// </param>
        /// <param name="handler">
        ///   The <see cref="IEventHandler"/> object to use to emit event notifications.
        /// </param>
        protected Node(NodeType type, IEngineDataStore store, IDateTimeProvider clock, IEventHandler handler)
        {
            Assert.ArgumentIsDefined(type, nameof(type));
            Assert.ArgumentIsNotNull(store, nameof(store));

            this._id = NodeIdentifier.Generate(type);
            this._clock = (clock ?? UtcDateTimeProvider.Default);
            this._dispatcher = new EventDispatcher(type, this._id, this._clock, handler);
            this._store = store;
        }


        protected sealed class EventDispatcher
        {
            private readonly NodeType _type;
            private readonly string _id;
            private readonly IDateTimeProvider _clock;
            private readonly IEventHandler _handler;


            internal EventDispatcher(NodeType type, string nodeId, IDateTimeProvider clock, IEventHandler handler)
            {
                Debug.Assert(nodeId != null);
                Debug.Assert(clock != null);

                this._type = type;
                this._id = nodeId;
                this._clock = clock;
                this._handler = handler;
            }

            internal async Task DispatchEventAsync(EventSeverity severity, string jobId, Guid parititonId, string description, CancellationToken cancellation)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(description != null);

                if (this._handler != null)
                {
                    EventDescriptor descriptor = new EventDescriptor
                    {
                        Timestamp = this._clock.GetDateTime(),
                        Source = this._type,
                        Severity = severity,
                        EngineId = this._id,
                        JobId = jobId,
                        PartitionId = parititonId,
                        Description = description
                    };

                    await this._handler.HandleEventAsync(descriptor, cancellation);
                }
            }

            internal async Task DispatchEventAsync(EventSeverity severity, string jobId, Guid parititonId, string description, Exception exception, CancellationToken cancellation)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(description != null);

                if (this._handler != null)
                {
                    EventDescriptor descriptor = new EventDescriptor
                    {
                        Timestamp = this._clock.GetDateTime(),
                        Source = this._type,
                        Severity = severity,
                        EngineId = this._id,
                        JobId = jobId,
                        PartitionId = parititonId,
                        Description = description,
                        Exception = exception
                    };

                    await this._handler.HandleEventAsync(descriptor, cancellation);
                }
            }
        }
    }
}
