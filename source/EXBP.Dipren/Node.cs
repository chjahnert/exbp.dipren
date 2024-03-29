﻿
using System.Diagnostics;

using EXBP.Dipren.Data;
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements a base class for various node types such as the scheduler and the processing engine.
    /// </summary>
    public abstract class Node
    {
        private readonly string _id;
        private readonly IEngineDataStore _store;
        private readonly ITimestampProvider _clock;
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
        ///   A <see cref="ITimestampProvider"/> object that can be used to generate timestamp values.
        /// </value>
        protected ITimestampProvider Clock => this._clock;

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
        ///   The <see cref="ITimestampProvider"/> to use to generate timestamps.
        /// </param>
        /// <param name="handler">
        ///   The <see cref="IEventHandler"/> object to use to emit event notifications.
        /// </param>
        protected Node(NodeType type, IEngineDataStore store, ITimestampProvider clock, IEventHandler handler)
        {
            Assert.ArgumentIsDefined(type, nameof(type));
            Assert.ArgumentIsNotNull(store, nameof(store));

            this._id = NodeIdentifier.Generate(type);
            this._clock = (clock ?? UtcTimestampProvider.Default);
            this._dispatcher = new EventDispatcher(type, this._id, this._clock, handler);
            this._store = store;
        }


        /// <summary>
        ///   Dispatches event notifications to the event handler.
        /// </summary>
        protected sealed class EventDispatcher
        {
            private readonly NodeType _type;
            private readonly string _id;
            private readonly ITimestampProvider _clock;
            private readonly IEventHandler _handler;


            /// <summary>
            ///   Initializes a new instance of the <see cref="EventDispatcher"/> type.
            /// </summary>
            /// <param name="type">
            ///   The type of the node generating the events.
            /// </param>
            /// <param name="nodeId">
            ///   The unique identifier of the node generating the events.
            /// </param>
            /// <param name="clock">
            ///   The <see cref="ITimestampProvider"/> object to use to generate timestamp values.
            /// </param>
            /// <param name="handler">
            ///   The <see cref="IEventHandler"/> object to dispatch the events to; or <see langword="null"/> to
            ///   discard the events.
            /// </param>
            public EventDispatcher(NodeType type, string nodeId, ITimestampProvider clock, IEventHandler handler)
            {
                Debug.Assert(nodeId != null);
                Debug.Assert(clock != null);

                this._type = type;
                this._id = nodeId;
                this._clock = clock;
                this._handler = handler;
            }

            /// <summary>
            ///   Dispatches an event related to a job and a partition.
            /// </summary>
            /// <param name="severity">
            ///   A <see cref="EventSeverity"/> value indicating the severity of the event.
            /// </param>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job the event is related to.
            /// </param>
            /// <param name="description">
            ///   A description of the event.
            /// </param>
            internal void DispatchEvent(EventSeverity severity, string jobId, string description)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(description != null);

                if (this._handler != null)
                {
                    EventDescriptor descriptor = new EventDescriptor
                    {
                        Timestamp = this._clock.GetCurrentTimestamp(),
                        Source = this._type,
                        Severity = severity,
                        EngineId = this._id,
                        JobId = jobId,
                        Description = description
                    };

                    this._handler.HandleEvent(descriptor);
                }
            }

            /// <summary>
            ///   Dispatches an event related to a job and a partition.
            /// </summary>
            /// <param name="severity">
            ///   A <see cref="EventSeverity"/> value indicating the severity of the event.
            /// </param>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job the event is related to.
            /// </param>
            /// <param name="description">
            ///   A description of the event.
            /// </param>
            /// <param name="exception">
            ///   The exception providing further information about the event; or <see langword="null"/> if not
            ///   available.
            /// </param>
            internal void DispatchEvent(EventSeverity severity, string jobId, string description, Exception exception)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(description != null);

                if (this._handler != null)
                {
                    EventDescriptor descriptor = new EventDescriptor
                    {
                        Timestamp = this._clock.GetCurrentTimestamp(),
                        Source = this._type,
                        Severity = severity,
                        EngineId = this._id,
                        JobId = jobId,
                        Description = description,
                        Exception = exception
                    };

                    this._handler.HandleEvent(descriptor);
                }
            }

            /// <summary>
            ///   Dispatches an event related to a job and a partition.
            /// </summary>
            /// <param name="severity">
            ///   A <see cref="EventSeverity"/> value indicating the severity of the event.
            /// </param>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job the event is related to.
            /// </param>
            /// <param name="partitonId">
            ///   The unique identifier of the partition the event is related to.
            /// </param>
            /// <param name="description">
            ///   A description of the event.
            /// </param>
            internal void DispatchEvent(EventSeverity severity, string jobId, Guid partitonId, string description)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(description != null);

                if (this._handler != null)
                {
                    EventDescriptor descriptor = new EventDescriptor
                    {
                        Timestamp = this._clock.GetCurrentTimestamp(),
                        Source = this._type,
                        Severity = severity,
                        EngineId = this._id,
                        JobId = jobId,
                        PartitionId = partitonId,
                        Description = description
                    };

                    this._handler.HandleEvent(descriptor);
                }
            }

            /// <summary>
            ///   Dispatches an event related to a job and a partition.
            /// </summary>
            /// <param name="severity">
            ///   A <see cref="EventSeverity"/> value indicating the severity of the event.
            /// </param>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job the event is related to.
            /// </param>
            /// <param name="partitonId">
            ///   The unique identifier of the partition the event is related to.
            /// </param>
            /// <param name="description">
            ///   A description of the event.
            /// </param>
            /// <param name="exception">
            ///   The exception providing further information about the event; or <see langword="null"/> if not
            ///   available.
            /// </param>
            internal void DispatchEvent(EventSeverity severity, string jobId, Guid partitonId, string description, Exception exception)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(description != null);

                if (this._handler != null)
                {
                    EventDescriptor descriptor = new EventDescriptor
                    {
                        Timestamp = this._clock.GetCurrentTimestamp(),
                        Source = this._type,
                        Severity = severity,
                        EngineId = this._id,
                        JobId = jobId,
                        PartitionId = partitonId,
                        Description = description,
                        Exception = exception
                    };

                    this._handler.HandleEvent(descriptor);
                }
            }
        }
    }
}
