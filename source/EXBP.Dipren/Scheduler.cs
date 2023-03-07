
using System.Diagnostics;
using System.Globalization;

using EXBP.Dipren.Data;
using EXBP.Dipren.Diagnostics;
using EXBP.Dipren.Telemetry;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements methods to schedule, cancel, and monitor distributed processing jobs.
    /// </summary>
    public class Scheduler : Node
    {
        private readonly Events _events;


        /// <summary>
        ///   Initializes a new instance of the <see cref="Scheduler"/> class.
        /// </summary>
        /// <param name="store">
        ///   The <see cref="IEngineDataStore"/> to use.
        /// </param>
        /// <param name="clock">
        ///   The <see cref="ITimestampProvider"/> to use to generate timestamps.
        /// </param>
        /// <param name="handler">
        ///   The <see cref="IEventHandler"/> object to use to emit event notifications.
        /// </param>
        internal Scheduler(IEngineDataStore store, ITimestampProvider clock, IEventHandler handler) : base(NodeType.Scheduler, store, clock, handler)
        {
            this._events = new Events(this.Dispatcher);
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="Scheduler"/> class.
        /// </summary>
        /// <param name="store">
        ///   The <see cref="IEngineDataStore"/> to use.
        /// </param>
        /// <param name="handler">
        ///   The <see cref="IEventHandler"/> object to use to emit event notifications.
        /// </param>
        public Scheduler(IEngineDataStore store, IEventHandler handler = null) : this(store, UtcTimestampProvider.Default, handler)
        {
            this._events = new Events(this.Dispatcher);
        }


        /// <summary>
        ///   Schedules a distributed processing job.
        /// </summary>
        /// <typeparam name="TKey">
        ///   The type of the item key.
        /// </typeparam>
        /// <typeparam name="TItem">
        ///   The type of items to process.
        /// </typeparam>
        /// <param name="job">
        ///   The job to schedule for distributed processing.
        /// </param>
        /// <param name="settings">
        ///   The job settings to use.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> object that represents the asynchronous operation.
        /// </returns>
        public async Task ScheduleAsync<TKey, TItem>(Job<TKey, TItem> job, Settings settings, CancellationToken cancellation = default)
        {
            Assert.ArgumentIsNotNull(job, nameof(job));

            Job entry = await this.CreateJobEntryAsync(job, settings, cancellation);

            try
            {
                await this.CreatePartitionEntryAsync(job, settings, cancellation);
                await this.MarkJobAsReadyAsync(entry.Id, cancellation);
            }
            catch (Exception ex)
            {
                await this.MarkJobAsFailedAsync(entry.Id, ex, cancellation);

                throw;
            }
        }

        /// <summary>
        ///   Gets a status report of the job with the specified unique identifier.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the job.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="StatusReport"/> object that represents the asynchronous
        ///   operation.
        /// </returns>
        public async Task<StatusReport> GetStatusReportAsync(string id, CancellationToken cancellation = default)
        {
            DateTime timestamp = this.Clock.GetCurrentTimestamp();

            StatusReport result = await this.Store.RetrieveJobStatusReportAsync(id, timestamp, cancellation);

            return result;
        }

        /// <summary>
        ///   Creates a job entry for the specified job in the engine data store.
        /// </summary>
        /// <typeparam name="TKey">
        ///   The type of the item key.
        /// </typeparam>
        /// <typeparam name="TItem">
        ///   The type of items to process.
        /// </typeparam>
        /// <param name="job">
        ///   The <see cref="Job{TKey, TItem}"/> object for which to create the entry.
        /// </param>
        /// <param name="settings">
        ///   The job settings to use.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Job"/> object that represents the asynchronous
        ///   operation.
        /// </returns>
        private async Task<Job> CreateJobEntryAsync<TKey, TItem>(Job<TKey, TItem> job, Settings settings, CancellationToken cancellation)
        {
            DateTime timestamp = this.Clock.GetCurrentTimestamp();
            Job result = new Job(job.Id, timestamp, timestamp, JobState.Initializing, settings.BatchSize, settings.Timeout, settings.ClockDrift);

            await this._events.RaiseCreatingJobAsync(job.Id, cancellation);

            await this.Store.InsertJobAsync(result, cancellation);

            await this._events.RaiseJobCreatedAsync(job.Id, cancellation);

            return result;
        }

        /// <summary>
        ///   Creates a partition entry for the specified job in the engine data store.
        /// </summary>
        /// <typeparam name="TKey">
        ///   The type of the item key.
        /// </typeparam>
        /// <typeparam name="TItem">
        ///   The type of items to process.
        /// </typeparam>
        /// <param name="job">
        ///   The <see cref="Job{TKey, TItem}"/> object for which to create the entry.
        /// </param>
        /// <param name="settings">
        ///   The job settings to use.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Partition"/> object that represents the asynchronous
        ///   operation.
        /// </returns>
        private async Task<Partition> CreatePartitionEntryAsync<TKey, TItem>(Job<TKey, TItem> job, Settings settings, CancellationToken cancellation)
        {
            Debug.Assert(job != null);
            Debug.Assert(settings != null);

            Guid partitionId = Guid.NewGuid();

            await this._events.RaiseCreatingInitialPartitionAsync(job.Id, partitionId, cancellation);
            await this._events.RaiseRetrievingRangeBoundariesAsync(job.Id, partitionId, cancellation);

            Stopwatch stopwatch = Stopwatch.StartNew();

            Range<TKey> range = await job.Source.GetEntireRangeAsync(cancellation);

            stopwatch.Stop();

            await this._events.RaiseRangeBoundariesRetrievedAsync(job.Id, partitionId, job.Serializer, range.First, range.Last, stopwatch.Elapsed, cancellation);

            await this._events.RaiseEstimatingRangeSizeAsync(job.Id, partitionId, cancellation);

            stopwatch.Restart();

            long remaining = await job.Source.EstimateRangeSizeAsync(range, cancellation);

            stopwatch.Stop();

            await this._events.RaiseRangeSizeEstimatedAsync(job.Id, partitionId, remaining, stopwatch.Elapsed, cancellation);

            //
            // When a split request is honored, the size of two key ranges is estimated. The timeout value should allow
            // for both range size estimations to complete.
            //

            if (stopwatch.Elapsed >= (settings.Timeout / 2))
            {
                await this._events.RaiseTimeoutValueTooLowAsync(job.Id, partitionId, cancellation);
            }

            DateTime timestampPartitionCreated = this.Clock.GetCurrentTimestamp();
            Partition<TKey> partition = new Partition<TKey>(partitionId, job.Id, null, timestampPartitionCreated, timestampPartitionCreated, range, default, 0L, remaining, false, 0.0, null);
            Partition result = partition.ToEntry(job.Serializer);

            await this.Store.InsertPartitionAsync(result, cancellation);

            await this._events.RaiseInitialPartitionCreatedAsync(job.Id, partitionId, cancellation);

            return result;
        }

        /// <summary>
        ///   Marks a job entry as ready.
        /// </summary>
        /// <param name="jobId">
        ///   The unique identifier of the job to update.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Job"/> object that represents the asynchronous
        ///   operation.
        /// </returns>
        private async Task<Job> MarkJobAsReadyAsync(string jobId, CancellationToken cancellation)
        {
            DateTime timestamp = this.Clock.GetCurrentTimestamp();
            Job result = await this.Store.MarkJobAsReadyAsync(jobId, timestamp, cancellation);

            return result;
        }

        /// <summary>
        ///   Marks a job entry as failed.
        /// </summary>
        /// <param name="jobId">
        ///   The unique identifier of the job to update.
        /// </param>
        /// <param name="exception">
        ///   The exception, if available, that provides information about the error.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Job"/> object that represents the asynchronous
        ///   operation.
        /// </returns>
        private async Task<Job> MarkJobAsFailedAsync(string jobId, Exception exception, CancellationToken cancellation)
        {
            DateTime timestamp = this.Clock.GetCurrentTimestamp();
            Job result = await this.Store.MarkJobAsFailedAsync(jobId, timestamp, exception?.Message, cancellation);

            return result;
        }


        /// <summary>
        ///   Implements methods for raising <see cref="Scheduler"/> events.
        /// </summary>
        private sealed class Events
        {
            private readonly EventDispatcher _dispatcher;


            /// <summary>
            ///   Initializes a new instance of the <see cref="Events"/> class.
            /// </summary>
            /// <param name="dispatcher">
            ///   The <see cref="Node.EventDispatcher"/> to use to dispatch events.
            /// </param>
            internal Events(EventDispatcher dispatcher)
            {
                Debug.Assert(dispatcher != null);

                this._dispatcher = dispatcher;
            }


            /// <summary>
            ///   Raises the event when a distributed processing job is being created.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="cancellation">
            ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
            ///   canceled.
            /// </param>
            /// <returns>
            ///   A <see cref="Task"/> object that represents the asynchronous operation.
            /// </returns>
            internal async Task RaiseCreatingJobAsync(string jobId, CancellationToken cancellation)
            {
                Debug.Assert(jobId != null);

                await this._dispatcher.DispatchEventAsync(EventSeverity.Information, jobId, SchedulerResources.EventCreatingJob, cancellation);
            }

            /// <summary>
            ///   Raises the event when a distributed processing job has been created.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="cancellation">
            ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
            ///   canceled.
            /// </param>
            /// <returns>
            ///   A <see cref="Task"/> object that represents the asynchronous operation.
            /// </returns>
            internal async Task RaiseJobCreatedAsync(string jobId, CancellationToken cancellation)
            {
                Debug.Assert(jobId != null);

                await this._dispatcher.DispatchEventAsync(EventSeverity.Information, jobId, SchedulerResources.EventJobCreated, cancellation);
            }

            /// <summary>
            ///   Raises the event when the initial partition for the distributed processing job is being created.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            /// <param name="cancellation">
            ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
            ///   canceled.
            /// </param>
            /// <returns>
            ///   A <see cref="Task"/> object that represents the asynchronous operation.
            /// </returns>
            internal async Task RaiseCreatingInitialPartitionAsync(string jobId, Guid partitionId, CancellationToken cancellation)
            {
                Debug.Assert(jobId != null);

                await this._dispatcher.DispatchEventAsync(EventSeverity.Information, jobId, partitionId, SchedulerResources.EventCreatingInitialPartition, cancellation);
            }

            /// <summary>
            ///   Raises the event when the boundaries for the partition are being retrieved.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            /// <param name="cancellation">
            ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
            ///   canceled.
            /// </param>
            /// <returns>
            ///   A <see cref="Task"/> object that represents the asynchronous operation.
            /// </returns>
            internal async Task RaiseRetrievingRangeBoundariesAsync(string jobId, Guid partitionId, CancellationToken cancellation)
            {
                Debug.Assert(jobId != null);

                await this._dispatcher.DispatchEventAsync(EventSeverity.Information, jobId, partitionId, SchedulerResources.EventRetrievingRangeBoundaries, cancellation);
            }

            /// <summary>
            ///   Raises the event when the boundaries for the partition have been retrieved.
            /// </summary>
            /// <typeparam name="TKey">
            ///   The type of the item key.
            /// </typeparam>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            /// <param name="serializer">
            ///   The <see cref="IKeySerializer{TKey}"/> of type <typeparamref name="TKey"/> that is used to serialize
            ///   keys.
            /// </param>
            /// <param name="first">
            ///   The first key in the range.
            /// </param>
            /// <param name="last">
            ///   The last key in the range.
            /// </param>
            /// <param name="duration">
            ///   The time it took to retrieve the range boundaries.
            /// </param>
            /// <param name="cancellation">
            ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
            ///   canceled.
            /// </param>
            /// <returns>
            ///   A <see cref="Task"/> object that represents the asynchronous operation.
            /// </returns>
            internal async Task RaiseRangeBoundariesRetrievedAsync<TKey>(string jobId, Guid partitionId, IKeySerializer<TKey> serializer, TKey first, TKey last, TimeSpan duration, CancellationToken cancellation)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(serializer != null);
                Debug.Assert(first != null);
                Debug.Assert(last != null);
                Debug.Assert(duration >= TimeSpan.Zero);

                string serializedFirst = serializer.Serialize(first);
                string serializedLast = serializer.Serialize(last);

                string message = string.Format(CultureInfo.InvariantCulture, SchedulerResources.EventRangeBoundariesRetrieved, serializedFirst, serializedLast, duration.TotalMilliseconds);

                await this._dispatcher.DispatchEventAsync(EventSeverity.Information, jobId, partitionId, message, cancellation);
            }

            /// <summary>
            ///   Raises the event when the size of a key range is being estimated.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            /// <param name="cancellation">
            ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
            ///   canceled.
            /// </param>
            /// <returns>
            ///   A <see cref="Task"/> object that represents the asynchronous operation.
            /// </returns>
            internal async Task RaiseEstimatingRangeSizeAsync(string jobId, Guid partitionId, CancellationToken cancellation)
            {
                Debug.Assert(jobId != null);

                await this._dispatcher.DispatchEventAsync(EventSeverity.Information, jobId, partitionId, SchedulerResources.EventEstimatingRangeSize, cancellation);
            }

            /// <summary>
            ///   Raises the event when the size of a key range has been estimated.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            /// <param name="count">
            ///   The number of keys in the range.
            /// </param>
            /// <param name="duration">
            ///   The time it took to complete the operation.
            /// </param>
            /// <param name="cancellation">
            ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
            ///   canceled.
            /// </param>
            /// <returns>
            ///   A <see cref="Task"/> object that represents the asynchronous operation.
            /// </returns>
            internal async Task RaiseRangeSizeEstimatedAsync(string jobId, Guid partitionId, long count, TimeSpan duration, CancellationToken cancellation)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(count >= 0);
                Debug.Assert(duration >= TimeSpan.Zero);

                string message = string.Format(CultureInfo.InvariantCulture, SchedulerResources.EventRangeSizeEstimated, count, duration.TotalMilliseconds);

                await this._dispatcher.DispatchEventAsync(EventSeverity.Information, jobId, partitionId, message, cancellation);
            }

            /// <summary>
            ///   Raises the event when the duration of creating a partition exceeded that configured timeout value.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            /// <param name="cancellation">
            ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
            ///   canceled.
            /// </param>
            /// <returns>
            ///   A <see cref="Task"/> object that represents the asynchronous operation.
            /// </returns>
            internal async Task RaiseTimeoutValueTooLowAsync(string jobId, Guid partitionId, CancellationToken cancellation)
            {
                Debug.Assert(jobId != null);

                await this._dispatcher.DispatchEventAsync(EventSeverity.Warning, jobId, partitionId, SchedulerResources.EventTimeoutValueTooLow, cancellation);
            }

            /// <summary>
            ///   Raises the event when the initial partition for a distributed processing job has been created.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            /// <param name="cancellation">
            ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
            ///   canceled.
            /// </param>
            /// <returns>
            ///   A <see cref="Task"/> object that represents the asynchronous operation.
            /// </returns>
            internal async Task RaiseInitialPartitionCreatedAsync(string jobId, Guid partitionId, CancellationToken cancellation)
            {
                Debug.Assert(jobId != null);

                await this._dispatcher.DispatchEventAsync(EventSeverity.Information, jobId, partitionId, SchedulerResources.EventInitialPartitionCreated, cancellation);
            }
        }
    }
}
