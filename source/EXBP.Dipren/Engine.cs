
using EXBP.Dipren.Data;
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements a processing engine that can iterate over a large set of ordered items in a distributed fashion.
    /// </summary>
    public class Engine
    {
        private readonly string _id;
        private readonly IEngineDataStore _store;
        private readonly IDateTimeProvider _clock;
        private readonly Configuration _configuration;


        /// <summary>
        ///   Gets the configuration settings for the current distributed processing engine instance.
        /// </summary>
        public Configuration Configuration => this._configuration;

        /// <summary>
        ///   Gets the unique identifier of the current engine.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains the unique identifier of the current engine.
        /// </value>
        public string Identity => this._id;


        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="store">
        ///   The <see cref="IEngineDataStore"/> to use.
        /// </param>
        /// <param name="configuration">
        ///   The configuration settings to use.
        /// </param>
        internal Engine(IEngineDataStore store, IDateTimeProvider clock, Configuration configuration)
        {
            Assert.ArgumentIsNotNull(store, nameof(store));
            Assert.ArgumentIsNotNull(clock, nameof(clock));
            Assert.ArgumentIsNotNull(configuration, nameof(configuration));

            this._store = store;
            this._clock = clock;
            this._configuration = configuration;
            this._id = EngineIdentifier.Generate();
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
        public Engine(IEngineDataStore store, Configuration configuration) : this(store, UtcDateTimeProvider.Default, configuration)
        {
        }

        /// <summary>
        ///   Executes a distributed processing job.
        /// </summary>
        /// <typeparam name="TKey">
        ///   The type of the item key.
        /// </typeparam>
        /// <typeparam name="TItem">
        ///   The type of items to process.
        /// </typeparam>
        /// <param name="job">
        ///   The job to start.
        /// </param>
        /// <param name="wait">
        ///   <see langword="true"/> to wait for the job to be ready; otherwise, <see langword="false"/>.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> that represents the asynchronous operation and can be used to access the result.
        /// </returns>
        /// <exception cref="JobNotScheduledException">
        ///   Argument <paramref name="wait"/> is <see langword="false"/> and the job has not yet been scheduled for
        ///   processing.
        /// </exception>
        /// <remarks>
        ///   <para>
        ///     If <paramref name="wait"/> is <see langword="true"/> and the job is not yet scheduled, the method will
        ///     wait for the job to be scheduled.
        ///   </para>
        /// </remarks>
        public async Task RunAsync<TKey, TItem>(Job<TKey, TItem> job, bool wait, CancellationToken cancellation) where TKey : IComparable<TKey>
        {
            Assert.ArgumentIsNotNull(job, nameof(job));

            Job retrieved = null;

            try
            {
                retrieved = await this._store.RetrieveJobAsync(job.Id, cancellation);
            }
            catch (UnknownIdentifierException ex)
            {
                if (wait == false)
                {
                    throw new JobNotScheduledException(EngineResources.NoJobScheduledWithSpecifiedIdentifier, job.Id, ex);
                }
            }

            //
            // If waiting was requested or the job is still initializing, we wait for the job to be scheduled and
            // become ready for processing.
            //

            while ((retrieved == null) || (retrieved.State == JobState.Initializing))
            {
                await Task.Delay(this._configuration.PollingInterval, cancellation);

                try
                {
                    retrieved = await this._store.RetrieveJobAsync(job.Id, cancellation);
                }
                catch (JobNotScheduledException)
                {
                }
            }

            //
            // Processing is only started if the scheduled job is in either Ready or Processing state.
            //

            while ((retrieved.State == JobState.Ready) || (retrieved.State == JobState.Processing))
            {
                //
                // Acquire a partition that is ready to be processed or request an existing partition to be split.
                //

                Partition<TKey> partition = await this.TryAcquirePartitionAsync(job, cancellation);

                if (partition != null)
                {
                    throw new NotImplementedException();
                }
                else
                {
                    await Task.Delay(this._configuration.PollingInterval, cancellation);
                }

                //
                // Flow:
                //
                // 1. Check if there are any free partitions.
                // 2. If there are no free partitions, check if there are any abandoned partitions.
                // 3. If there are no free or abandoned partitions, request (the largest) partition to be split.
                // 4. Take ownership of the partition.
                // 5. Start processing the partition in a loop.
                //    a. Process the next batch of keys
                //    b. Record progress
                //    c. If requested, split the current partition
                // 6. Once the current partition is completed, repeat from step 1 until all keys are processed.
                // 7. Mark the job completed.
                //
                // Questions:
                //
                //  - Should partitions smaller than the batch size be split?
                //    Could be a configuration option. Default: yes.
                //  - How long to wait after a split was requested?
                //    Could be a configuration option. Default: 2 seconds.
                //
            }
        }

        /// <summary>
        ///   Tries to acquire a partition to be processed.
        /// </summary>
        /// <typeparam name="TKey">
        ///   The type of the item key.
        /// </typeparam>
        /// <typeparam name="TItem">
        ///   The type of items to process.
        /// </typeparam>
        /// <param name="job">
        ///   The job being processed.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Partition{TKey}"/> object that represents the asynchronous
        ///   operation. The <see cref="Task{TResult}.Result"/> property contains the acquired partition if succeeded;
        ///   otherwise, <see langword="null"/>.
        /// </returns>
        private async Task<Partition<TKey>> TryAcquirePartitionAsync<TKey, TItem>(Job<TKey, TItem> job, CancellationToken cancellation) where TKey : IComparable<TKey>
        {
            Assert.ArgumentIsNotNull(job, nameof(job));

            DateTime now = this._clock.GetDateTime();
            DateTime cut = (now - this._configuration.BatchProcessingTimeout - this._configuration.MaximumClockDrift);

            Partition acquired = await this._store.TryAcquirePartitionsAsync(job.Id, this.Identity, now, cut, cancellation);

            Partition<TKey> result = null;

            if (acquired != null)
            {
                result = acquired.ToPartition(job.Serializer);
            }
            else
            {
                await this._store.TryRequestSplitAsync(job.Id, cut, cancellation);
            }

            return result;
        }
    }
}
