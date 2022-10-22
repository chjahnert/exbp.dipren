
using System.Diagnostics;

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
        internal Engine(IEngineDataStore store, IDateTimeProvider clock, Configuration configuration = null)
        {
            Assert.ArgumentIsNotNull(store, nameof(store));
            Assert.ArgumentIsNotNull(clock, nameof(clock));

            this._store = store;
            this._clock = clock;
            this._configuration = (configuration ?? new Configuration());
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
        public Engine(IEngineDataStore store, Configuration configuration = null) : this(store, UtcDateTimeProvider.Default, configuration)
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
            //    c. Check job state.
            //    d. If requested, split the current partition
            // 6. Once the current partition is completed, repeat from step 1 until all keys are processed.
            // 7. Mark the job completed.
            //
            // Questions:
            //
            //  - Should partitions smaller than the batch size be split?
            //    Could be a configuration option. Default: yes.
            //  - How to handle the condition when the scheduler crashed right after the job was inserted?
            //    Could be a configuration option like job initialization timeout with a default of 15 minutes.
            //

            Job persisted = null;

            try
            {
                persisted = await this._store.RetrieveJobAsync(job.Id, cancellation);
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

            while ((persisted == null) || (persisted.State == JobState.Initializing))
            {
                await Task.Delay(this._configuration.PollingInterval, cancellation);

                try
                {
                    persisted = await this._store.RetrieveJobAsync(job.Id, cancellation);
                }
                catch (UnknownIdentifierException)
                {
                }
            }

            //
            // Processing is only started if the scheduled job is in either Ready or Processing state.
            //

            while ((persisted?.State == JobState.Ready) || (persisted?.State == JobState.Processing))
            {
                //
                // Acquire a partition that is ready to be processed or request an existing partition to be split.
                //

                Partition<TKey> partition = await this.TryAcquirePartitionAsync(job, cancellation);

                if (partition != null)
                {
                    try
                    {
                        await this.ProcessPartitionAsync(job, partition, cancellation);
                    }
                    catch (LockException)
                    {
                        //
                        // The partition being processed was stolen by another processing node.
                        //
                    }
                }
                else
                {
                    //
                    // TODO: Is any work left?
                    //

                    bool completed = false;

                    if (completed == true)
                    {
                        //
                        // TODO: Mark the job as completed.
                        //
                    }
                    else
                    {
                        //
                        // If a partition could not be acquired, wait the configured amount of time and check if the job
                        // has not completed, failed, or was deleted in the meanwhile.
                        //

                        await Task.Delay(this._configuration.PollingInterval, cancellation);

                        try
                        {
                            persisted = await this._store.RetrieveJobAsync(job.Id, cancellation);
                        }
                        catch (UnknownIdentifierException)
                        {
                        }
                    }
                }
            }
        }

        /// <summary>
        ///   Processes the specified partition until it is completed.
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
        /// <param name="partition">
        ///   The partition to process.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> object that represents the asynchronous operation.
        /// </returns>
        private async Task ProcessPartitionAsync<TKey, TItem>(Job<TKey, TItem> job, Partition<TKey> partition, CancellationToken cancellation) where TKey : IComparable<TKey>
        {
            Assert.ArgumentIsNotNull(job, nameof(job));
            Assert.ArgumentIsNotNull(partition, nameof(partition));

            //
            // 1. Fetch the next batch of items to be processed from the data source.
            // 2. Process the batch.
            // 3. Try updating the partition with the progress made while reading the split request flag.
            //    The partition can only be updated if it is still owned by the current processing node.
            // 4. If a split was requested, create a new partition and save it along with the current partition in an
            //    atomic fashion. Must not update and insert if the current node no longer owns the partition.
            // 5. Repeat from step 1 until completed.
            //

            while (partition.IsCompleted == false)
            {
                Range<TKey> range = new Range<TKey>(partition.Position, partition.Range.Last, partition.Range.IsInclusive);
                int skip = ((partition.Processed == 0L) ? 0 : 1);

                IEnumerable<KeyValuePair<TKey, TItem>> batch = await job.Source.GetNextBatchAsync(range, skip, job.BatchSize, cancellation);

                IEnumerable<TItem> items = batch.Select(kvp => kvp.Value);

                await job.Processor.ProcessAsync(items, cancellation);

                long progress = batch.Count();
                bool completed = (progress < job.BatchSize);
                TKey position = batch.Last().Key;
                DateTime timestamp = this._clock.GetDateTime();

                partition = await this.ReportProgressAsync(job, partition, timestamp, position, progress, completed, cancellation);

                if (partition.IsSplitRequested == true)
                {
                    partition = await this.SplitPartitionAsync(job, partition, cancellation);
                }
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
            Debug.Assert(job != null);

            DateTime now = this._clock.GetDateTime();
            DateTime cut = (now - job.Timeout - this._configuration.MaximumClockDrift);

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

        /// <summary>
        ///   Updates a partition with the progress made.
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
        /// <param name="partition">
        ///   The partition to update.
        /// </param>
        /// <param name="timestamp">
        ///   The current timestamp.
        /// </param>
        /// <param name="position">
        ///   The key of the last item processed in the key range of the partition.
        /// </param>
        /// <param name="progress">
        ///   The number of items processed since the last progress update.
        /// </param>
        /// <param name="completed">
        ///   <see langword="true"/> if the partition is completed; otherwise, <see langword="false"/>.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Partition"/> object that represents the asynchronous
        ///   operation. The <see cref="Task{TResult}.Result"/> property contains the updated partition.
        /// </returns>
        /// <exception cref="LockException">
        ///   The current processing node no longer holds the lock on the partition.
        /// </exception>
        private async Task<Partition<TKey>> ReportProgressAsync<TKey, TItem>(Job<TKey, TItem> job, Partition<TKey> partition, DateTime timestamp, TKey position, long progress, bool completed, CancellationToken cancellation) where TKey : IComparable<TKey>
        {
            Debug.Assert(job != null);
            Debug.Assert(partition != null);
            Debug.Assert(partition.Owner == this.Identity);
            Debug.Assert(position != null);
            Debug.Assert(progress >= 0L);

            string sp = job.Serializer.Serialize(position);

            Partition updated = await this._store.ReportProgressAsync(partition.Id, this.Identity, timestamp, sp, progress, completed, cancellation);

            Partition<TKey> result = updated.ToPartition(job.Serializer);

            return result;
        }

        /// <summary>
        ///   Splits the specified partition.
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
        /// <param name="partition">
        ///   The partition to split.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Partition"/> object that represents the asynchronous
        ///   operation. The <see cref="Task{TResult}.Result"/> property contains the updated partition.
        /// </returns>
        private async Task<Partition<TKey>> SplitPartitionAsync<TKey, TItem>(Job<TKey, TItem> job, Partition<TKey> partition, CancellationToken cancellation) where TKey : IComparable<TKey>
        {
            Debug.Assert(partition != null);

            Range<TKey> remainingKeyRange = partition.GetRemainingKeyRange();
            Range<TKey> updatedKeyRange = job.Arithmetics.Split(remainingKeyRange, out Range<TKey> excludedKeyRange);

            Partition<TKey> result = partition;

            if (excludedKeyRange != null)
            {
                Task<long> remainingKeyRangeSize = job.Source.EstimateRangeSizeAsync(updatedKeyRange, cancellation);
                Task<long> excludedKeyRangeSize = job.Source.EstimateRangeSizeAsync(excludedKeyRange, cancellation);

                DateTime timestamp = this._clock.GetDateTime();

                Partition<TKey> updatedPartition = new Partition<TKey>(partition.Id, partition.JobId, partition.Owner, partition.Created, timestamp, remainingKeyRange, partition.Position, partition.Processed, await remainingKeyRangeSize, false, false);
                Partition updatedEntry = updatedPartition.ToEntry(job.Serializer);

                Guid id = Guid.NewGuid();
                Partition<TKey> expludedPartition = new Partition<TKey>(id, partition.JobId, null, timestamp, timestamp, excludedKeyRange, default, 0L, await excludedKeyRangeSize, false, false);
                Partition excludedEntry = expludedPartition.ToEntry(job.Serializer);

                await this._store.InsertSplitPartitionAsync(updatedEntry, excludedEntry, cancellation);

                result = updatedPartition;
            }

            return result;
        }
    }
}
