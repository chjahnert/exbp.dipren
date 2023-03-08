
using System.Diagnostics;
using System.Globalization;

using EXBP.Dipren.Data;
using EXBP.Dipren.Diagnostics;


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

            this._events.RaiseCreatingJob(job.Id);

            await this.Store.InsertJobAsync(result, cancellation);

            this._events.RaiseJobCreated(job.Id);

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

            this._events.RaiseCreatingInitialPartition(job.Id, partitionId);
            this._events.RaiseRetrievingRangeBoundaries(job.Id, partitionId);

            Stopwatch stopwatch = Stopwatch.StartNew();

            Range<TKey> range = await job.Source.GetEntireRangeAsync(cancellation);

            stopwatch.Stop();

            this._events.RaiseRangeBoundariesRetrieved(job.Id, partitionId, job.Serializer, range.First, range.Last, stopwatch.Elapsed);
            this._events.RaiseEstimatingRangeSize(job.Id, partitionId);

            stopwatch.Restart();

            long remaining = await job.Source.EstimateRangeSizeAsync(range, cancellation);

            stopwatch.Stop();

            this._events.RaiseRangeSizeEstimated(job.Id, partitionId, remaining, stopwatch.Elapsed);

            //
            // When a split request is honored, the size of two key ranges is estimated. The timeout value should allow
            // for both range size estimations to complete.
            //

            if (stopwatch.Elapsed >= (settings.Timeout / 2))
            {
                this._events.RaiseTimeoutValueTooLow(job.Id, partitionId);
            }

            DateTime timestampPartitionCreated = this.Clock.GetCurrentTimestamp();
            Partition<TKey> partition = new Partition<TKey>(partitionId, job.Id, null, timestampPartitionCreated, timestampPartitionCreated, range, default, 0L, remaining, false, 0.0, null);
            Partition result = partition.ToEntry(job.Serializer);

            await this.Store.InsertPartitionAsync(result, cancellation);

            this._events.RaiseInitialPartitionCreated(job.Id, partitionId);

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
            internal void RaiseCreatingJob(string jobId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, SchedulerResources.EventCreatingJob);
            }

            /// <summary>
            ///   Raises the event when a distributed processing job has been created.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            internal void RaiseJobCreated(string jobId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, SchedulerResources.EventJobCreated);
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
            internal void RaiseCreatingInitialPartition(string jobId, Guid partitionId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, partitionId, SchedulerResources.EventCreatingInitialPartition);
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
            internal void RaiseRetrievingRangeBoundaries(string jobId, Guid partitionId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, partitionId, SchedulerResources.EventRetrievingRangeBoundaries);
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
            internal void RaiseRangeBoundariesRetrieved<TKey>(string jobId, Guid partitionId, IKeySerializer<TKey> serializer, TKey first, TKey last, TimeSpan duration)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(serializer != null);
                Debug.Assert(first != null);
                Debug.Assert(last != null);
                Debug.Assert(duration >= TimeSpan.Zero);

                string serializedFirst = serializer.Serialize(first);
                string serializedLast = serializer.Serialize(last);

                string message = string.Format(CultureInfo.InvariantCulture, SchedulerResources.EventRangeBoundariesRetrieved, serializedFirst, serializedLast, duration.TotalMilliseconds);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, partitionId, message);
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
            internal void RaiseEstimatingRangeSize(string jobId, Guid partitionId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, partitionId, SchedulerResources.EventEstimatingRangeSize);
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
            internal void RaiseRangeSizeEstimated(string jobId, Guid partitionId, long count, TimeSpan duration)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(count >= 0);
                Debug.Assert(duration >= TimeSpan.Zero);

                string message = string.Format(CultureInfo.InvariantCulture, SchedulerResources.EventRangeSizeEstimated, count, duration.TotalMilliseconds);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, partitionId, message);
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
            internal void RaiseTimeoutValueTooLow(string jobId, Guid partitionId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Warning, jobId, partitionId, SchedulerResources.EventTimeoutValueTooLow);
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
            internal void RaiseInitialPartitionCreated(string jobId, Guid partitionId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, partitionId, SchedulerResources.EventInitialPartitionCreated);
            }
        }
    }
}
