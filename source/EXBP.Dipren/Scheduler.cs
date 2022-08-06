
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
        private readonly Configuration _configuration;


        /// <summary>
        ///   Gets the configuration settings for the current distributed processing engine instance.
        /// </summary>
        public Configuration Configuration => this._configuration;


        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="store">
        ///   The <see cref="IEngineDataStore"/> to use.
        /// </param>
        /// <param name="configuration">
        ///   The configuration settings to use.
        /// </param>
        internal Scheduler(IEngineDataStore store, IDateTimeProvider clock, Configuration configuration)
        {
            Assert.ArgumentIsNotNull(store, nameof(store));
            Assert.ArgumentIsNotNull(clock, nameof(clock));
            Assert.ArgumentIsNotNull(configuration, nameof(configuration));

            this._store = store;
            this._clock = clock;
            this._configuration = configuration;
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="store">
        ///   The <see cref="IEngineDataStore"/> to use.
        /// </param>
        /// <param name="configuration">
        ///   The configuration settings to use.
        /// </param>
        public Scheduler(IEngineDataStore store, Configuration configuration) : this(store, UtcDateTimeProvider.Default, configuration)
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

            Range<TKey> range = await job.Source.GetRangeAsync(cancellation);
            long remaining = await job.Source.EstimateRangeSizeAsync(range, cancellation);

            Guid id = Guid.NewGuid();
            DateTime timestamp = this._clock.GetDateTime();

            Partition<TKey> partition = new Partition<TKey>(id, job.Id, null, timestamp, timestamp, range, default, 0L, remaining);
            PartitionEntry entry = partition.ToEntry(job.Serializer);

            await this._store.InsertPartitionAsync(entry, cancellation);
        }
    }
}
