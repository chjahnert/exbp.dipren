
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
        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="store">
        ///   The <see cref="IEngineDataStore"/> to use.
        /// </param>
        /// <param name="handler">
        ///   The <see cref="IEventHandler"/> object to use to emit event notifications.
        /// </param>
        /// <param name="clock">
        ///   The <see cref="IDateTimeProvider"/> to use to generate timestamps.
        /// </param>
        internal Scheduler(IEngineDataStore store, IEventHandler handler, IDateTimeProvider clock) : base(NodeType.Scheduler, store, clock, handler)
        {
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="logger">
        ///   The <see cref="IEventHandler"/> object to use to emit event notifications.
        /// </param>
        /// <param name="store">
        ///   The <see cref="IEngineDataStore"/> to use.
        /// </param>
        public Scheduler(IEngineDataStore store, IEventHandler logger = null) : this(store, logger, UtcDateTimeProvider.Default)
        {
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
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> object that represents the asynchronous operation.
        /// </returns>
        public async Task ScheduleAsync<TKey, TItem>(Job<TKey, TItem> job, CancellationToken cancellation) where TKey : IComparable<TKey>
        {
            Assert.ArgumentIsNotNull(job, nameof(job));

            Job entry = await this.CreateJobEntryAsync(job, cancellation);

            try
            {
                await this.CreatePartitionEntryAsync(job, cancellation);
                await this.MarkJobAsReadyAsync(entry.Id, cancellation);
            }
            catch (Exception ex)
            {
                await this.MarkJobAsFailedAsync(entry.Id, ex, cancellation);

                throw;
            }
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
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Job"/> object that represents the asynchronous
        ///   operation.
        /// </returns>
        private async Task<Job> CreateJobEntryAsync<TKey, TItem>(Job<TKey, TItem> job, CancellationToken cancellation) where TKey : IComparable<TKey>
        {
            DateTime timestamp = this.Clock.GetDateTime();
            Job result = new Job(job.Id, timestamp, timestamp, JobState.Initializing);

            await this.Dispatcher.DispatchEventAsync(EventSeverity.Debug, job.Id, "Creating job entry.", cancellation);

            await this.Store.InsertJobAsync(result, cancellation);

            await this.Dispatcher.DispatchEventAsync(EventSeverity.Debug, job.Id, "Job entry created.", cancellation);

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
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Partition"/> object that represents the asynchronous
        ///   operation.
        /// </returns>
        private async Task<Partition> CreatePartitionEntryAsync<TKey, TItem>(Job<TKey, TItem> job, CancellationToken cancellation) where TKey : IComparable<TKey>
        {
            Range<TKey> range = await job.Source.GetEntireRangeAsync(cancellation);
            long remaining = await job.Source.EstimateRangeSizeAsync(range, cancellation);

            Guid id = Guid.NewGuid();
            DateTime timestampPartitionCreated = this.Clock.GetDateTime();
            Partition<TKey> partition = new Partition<TKey>(id, job.Id, null, timestampPartitionCreated, timestampPartitionCreated, range, default, 0L, remaining, false, false);
            Partition result = partition.ToEntry(job.Serializer);

            await this.Store.InsertPartitionAsync(result, cancellation);

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
            DateTime timestamp = this.Clock.GetDateTime();
            Job result = await this.Store.UpdateJobAsync(jobId, timestamp, JobState.Ready, null, cancellation);

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
            DateTime timestamp = this.Clock.GetDateTime();
            Job result = await this.Store.UpdateJobAsync(jobId, timestamp, JobState.Failed, exception, cancellation);

            return result;
        }
    }
}
