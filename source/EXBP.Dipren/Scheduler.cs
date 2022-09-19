
using EXBP.Dipren.Data;
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements methods to schedule, cancel, and monitor distributed processing jobs.
    /// </summary>
    public class Scheduler
    {
        private readonly IEngineDataStore _store;
        private readonly IDateTimeProvider _clock;


        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="store">
        ///   The <see cref="IEngineDataStore"/> to use.
        /// </param>
        internal Scheduler(IEngineDataStore store, IDateTimeProvider clock)
        {
            Assert.ArgumentIsNotNull(store, nameof(store));
            Assert.ArgumentIsNotNull(clock, nameof(clock));

            this._store = store;
            this._clock = clock;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="store">
        ///   The <see cref="IEngineDataStore"/> to use.
        /// </param>
        public Scheduler(IEngineDataStore store) : this(store, UtcDateTimeProvider.Default)
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
                await this.MarkJobAsReadyAsync(entry, cancellation);
            }
            catch (Exception ex)
            {
                await this.MarkJobAsFailedAsync(entry, ex, cancellation);

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
            DateTime timestamp = this._clock.GetDateTime();
            Job result = new Job(job.Id, timestamp, timestamp, JobState.Initializing);

            await this._store.InsertJobAsync(result, cancellation);

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
            DateTime timestampPartitionCreated = this._clock.GetDateTime();
            Partition<TKey> partition = new Partition<TKey>(id, job.Id, null, timestampPartitionCreated, timestampPartitionCreated, range, default, 0L, remaining, false);
            Partition result = partition.ToEntry(job.Serializer);

            await this._store.InsertPartitionAsync(result, cancellation);

            return result;
        }

        /// <summary>
        ///   Marks a job entry as ready.
        /// </summary>
        /// <param name="template">
        ///   The job entry to use as a template for the updated job entry.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Job"/> object that represents the asynchronous
        ///   operation.
        /// </returns>
        private async Task<Job> MarkJobAsReadyAsync(Job template, CancellationToken cancellation)
        {
            DateTime timetamp = this._clock.GetDateTime();
            Job result = new Job(template.Id, template.Created, timetamp, JobState.Ready);

            await this._store.UpdateJobAsync(result, cancellation);

            return result;
        }

        /// <summary>
        ///   Marks a job entry as failed.
        /// </summary>
        /// <param name="template">
        ///   The job entry to use as a template for the updated job entry.
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
        private async Task<Job> MarkJobAsFailedAsync(Job template, Exception exception, CancellationToken cancellation)
        {
            DateTime timetamp = this._clock.GetDateTime();
            Job result = new Job(template.Id, template.Created, timetamp, JobState.Failed, exception);

            await this._store.UpdateJobAsync(result, cancellation);

            return result;
        }
    }
}
