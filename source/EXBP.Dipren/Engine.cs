
using System.Diagnostics;
using System.Globalization;

using EXBP.Dipren.Data;
using EXBP.Dipren.Diagnostics;
using EXBP.Dipren.Telemetry;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements a processing engine that can iterate over a large set of ordered items in a distributed fashion.
    /// </summary>
    public class Engine : Node
    {
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
        /// <param name="clock">
        ///   A <see cref="IDateTimeProvider"/> that can be used to generate timestamp values; or
        ///   <see langword="null"/> to use a <see cref="UtcDateTimeProvider"/> instance.
        /// </param>
        /// <param name="handler">
        ///   The <see cref="IEventHandler"/> object to use to emit event notifications; or <see langword="null"/> to
        ///   discard event notifications.
        /// </param>
        /// <param name="configuration">
        ///   The configuration settings to use; or <see langword="null"/> to use the default configuration settings.
        /// </param>
        internal Engine(IEngineDataStore store, IDateTimeProvider clock, IEventHandler handler = null, Configuration configuration = null) : base(NodeType.Engine, store, clock, handler)
        {
            this._configuration = (configuration ?? new Configuration());
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="store">
        ///   The <see cref="IEngineDataStore"/> to use.
        /// </param>
        /// <param name="handler">
        ///   The <see cref="IEventHandler"/> object to use to emit event notifications; or <see langword="null"/> to
        ///   discard event notifications.
        /// </param>
        /// <param name="configuration">
        ///   The configuration settings to use; or <see langword="null"/> to use the default configuration settings.
        /// </param>
        public Engine(IEngineDataStore store, IEventHandler handler = null, Configuration configuration = null) : base(NodeType.Engine, store, null, handler)
        {
            this._configuration = (configuration ?? new Configuration());
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
        public async Task RunAsync<TKey, TItem>(Job<TKey, TItem> job, bool wait, CancellationToken cancellation = default)
        {
            Assert.ArgumentIsNotNull(job, nameof(job));

            try
            {
                await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, EngineResources.EventJobStarted, cancellation);

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
                    persisted = await this.Store.RetrieveJobAsync(job.Id, cancellation);
                }
                catch (UnknownIdentifierException ex)
                {
                    await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, EngineResources.EventJobNotScheduled, cancellation);

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
                    await this.Dispatcher.DispatchEventAsync(EventSeverity.Debug, job.Id, EngineResources.EventWaitingForJobToBeReady, cancellation);

                    await Task.Delay(this._configuration.PollingInterval, cancellation);

                    try
                    {
                        persisted = await this.Store.RetrieveJobAsync(job.Id, cancellation);
                    }
                    catch (UnknownIdentifierException)
                    {
                    }
                }

                //
                // Mark the job as started if necessary.
                //

                if (persisted.State == JobState.Ready)
                {
                    persisted = await this.MarkJobAsStartedAsync(persisted.Id, cancellation);
                }

                await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, EngineResources.EventJobReady, cancellation);

                //
                // Processing is only started if the scheduled job is in either Ready or Processing state.
                //

                Settings settings = new Settings(persisted.BatchSize, persisted.Timeout);

                while (persisted?.State == JobState.Processing)
                {
                    //
                    // Acquire a partition that is ready to be processed or request an existing partition to be split.
                    //

                    Partition<TKey> partition = await this.TryAcquirePartitionAsync(job, settings, cancellation);

                    if (partition != null)
                    {
                        try
                        {
                            await this.ProcessPartitionAsync(job, partition, settings, cancellation);
                        }
                        catch (LockException)
                        {
                            //
                            // The partition being processed was taken by another processing node.
                            //

                            await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, partition.Id, EngineResources.EventPartitionTaken, cancellation);
                        }
                    }
                    else
                    {
                        long incomplete = await this.Store.CountIncompletePartitionsAsync(job.Id, cancellation);

                        if (incomplete == 0L)
                        {
                            persisted = await this.MarkJobAsCompletedAsync(persisted.Id, cancellation);

                            await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, EngineResources.EventJobCompleted, cancellation);
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
                                persisted = await this.Store.RetrieveJobAsync(job.Id, cancellation);
                            }
                            catch (UnknownIdentifierException)
                            {
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                await this.Dispatcher.DispatchEventAsync(EventSeverity.Error, job.Id, EngineResources.EventProcessingFailed, ex, cancellation);

                throw;
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
        private async Task ProcessPartitionAsync<TKey, TItem>(Job<TKey, TItem> job, Partition<TKey> partition, Settings settings, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(job, nameof(job));
            Assert.ArgumentIsNotNull(partition, nameof(partition));

            await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, partition.Id, EngineResources.EventProcessingPartition, cancellation);

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
                TKey first = ((partition.Processed == 0L) ? partition.Range.First : partition.Position);
                Range<TKey> range = new Range<TKey>(first, partition.Range.Last, partition.Range.IsInclusive);
                int skip = ((partition.Processed == 0L) ? 0 : 1);

                string descriptionRequestingNextBatch = string.Format(CultureInfo.InvariantCulture, EngineResources.EventRequestingNextBatch, settings.BatchSize, range.First, range.Last, skip);
                await this.Dispatcher.DispatchEventAsync(EventSeverity.Debug, job.Id, partition.Id, descriptionRequestingNextBatch, cancellation);

                Stopwatch stopwatch = Stopwatch.StartNew();

                IEnumerable<KeyValuePair<TKey, TItem>> batch = await job.Source.GetNextBatchAsync(range, skip, settings.BatchSize, cancellation);

                stopwatch.Stop();

                long progress = batch.Count();

                string descriptionBatchRetrieved = string.Format(CultureInfo.InvariantCulture, EngineResources.EventBatchRetrieved, progress, stopwatch.Elapsed.TotalMilliseconds);
                await this.Dispatcher.DispatchEventAsync(EventSeverity.Debug, job.Id, partition.Id, descriptionBatchRetrieved, cancellation);

                if (progress > 0L)
                {
                    IEnumerable<TItem> items = batch.Select(kvp => kvp.Value);

                    stopwatch.Restart();

                    await job.Processor.ProcessAsync(items, cancellation);

                    stopwatch.Stop();

                    string descriptionBatchProcessed = string.Format(CultureInfo.InvariantCulture, EngineResources.EventBatchProcessed, progress, stopwatch.Elapsed.TotalMilliseconds);
                    await this.Dispatcher.DispatchEventAsync(EventSeverity.Debug, job.Id, partition.Id, descriptionBatchProcessed, cancellation);
                }

                bool completed = (progress < settings.BatchSize);
                TKey position = ((progress == 0L) ? partition.Position : batch.Last().Key);
                DateTime timestamp = this.Clock.GetDateTime();

                partition = await this.ReportProgressAsync(job, partition, timestamp, position, progress, completed, cancellation);

                if (partition.IsSplitRequested == true)
                {
                    partition = await this.SplitPartitionAsync(job, partition, settings, cancellation);
                }
            }

            await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, partition.Id, EngineResources.EventPartitionCompleted, cancellation);
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
        /// <param name="settings">
        ///   The job settings to use.
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
        private async Task<Partition<TKey>> TryAcquirePartitionAsync<TKey, TItem>(Job<TKey, TItem> job, Settings settings, CancellationToken cancellation)
        {
            Debug.Assert(job != null);
            Debug.Assert(settings != null);

            await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, EngineResources.EventTryingToAcquirePartition, cancellation);

            DateTime now = this.Clock.GetDateTime();
            DateTime cut = (now - settings.Timeout - this._configuration.MaximumClockDrift);

            Partition acquired = await this.Store.TryAcquirePartitionAsync(job.Id, this.Id, now, cut, cancellation);

            Partition<TKey> result = null;

            if (acquired != null)
            {
                result = acquired.ToPartition(job.Serializer);

                await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, result.Id, EngineResources.EventPartitionAcquired, cancellation);
            }
            else
            {
                await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, EngineResources.EventPartitionNotAcquired, cancellation);
                await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, EngineResources.EventRequestingSplit, cancellation);

                bool succeeded = await this.Store.TryRequestSplitAsync(job.Id, cut, cancellation);

                await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, (succeeded ? EngineResources.EventSplitRequestSucceeded : EngineResources.EventSplitRequestFailed), cancellation);
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
        private async Task<Partition<TKey>> ReportProgressAsync<TKey, TItem>(Job<TKey, TItem> job, Partition<TKey> partition, DateTime timestamp, TKey position, long progress, bool completed, CancellationToken cancellation)
        {
            Debug.Assert(job != null);
            Debug.Assert(partition != null);
            Debug.Assert(partition.Owner == this.Id);
            Debug.Assert(position != null);
            Debug.Assert(progress >= 0L);

            string sp = job.Serializer.Serialize(position);

            Partition updated = await this.Store.ReportProgressAsync(partition.Id, this.Id, timestamp, sp, progress, completed, cancellation);

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
        /// <param name="settings">
        ///   The job settings to use.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="Partition"/> object that represents the asynchronous
        ///   operation. The <see cref="Task{TResult}.Result"/> property contains the updated partition.
        /// </returns>
        private async Task<Partition<TKey>> SplitPartitionAsync<TKey, TItem>(Job<TKey, TItem> job, Partition<TKey> partition, Settings settings, CancellationToken cancellation)
        {
            Debug.Assert(partition != null);

            await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, partition.Id, EngineResources.EventSplitRequested, cancellation);

            Range<TKey> remainingKeyRange = partition.GetRemainingKeyRange();
            Range<TKey> updatedKeyRange = job.Arithmetics.Split(remainingKeyRange, out Range<TKey> excludedKeyRange);

            Partition<TKey> result = partition;

            if (excludedKeyRange != null)
            {
                Stopwatch stopwatch = Stopwatch.StartNew();

                Task<long> excludedKeyRangeSize = job.Source.EstimateRangeSizeAsync(excludedKeyRange, cancellation);
                Task<long> updatedKeyRangeSize = job.Source.EstimateRangeSizeAsync(updatedKeyRange, cancellation);

                if ((await excludedKeyRangeSize >= settings.BatchSize) && (await updatedKeyRangeSize >= settings.BatchSize))
                {
                    DateTime timestamp = this.Clock.GetDateTime();

                    Partition<TKey> updatedPartition = new Partition<TKey>(partition.Id, partition.JobId, partition.Owner, partition.Created, timestamp, updatedKeyRange, partition.Position, partition.Processed, (await updatedKeyRangeSize - 1), false, false);
                    Partition updatedEntry = updatedPartition.ToEntry(job.Serializer);

                    Guid id = Guid.NewGuid();
                    Partition<TKey> excludedPartition = new Partition<TKey>(id, partition.JobId, null, timestamp, timestamp, excludedKeyRange, default, 0L, await excludedKeyRangeSize, false, false);
                    Partition excludedEntry = excludedPartition.ToEntry(job.Serializer);

                    await this.Store.InsertSplitPartitionAsync(updatedEntry, excludedEntry, cancellation);

                    result = updatedPartition;

                    string descriptionPartitionSplit = String.Format(CultureInfo.InvariantCulture, EngineResources.EventPartitionSplit, updatedPartition.Range.First, updatedPartition.Range.Last, excludedPartition.Id, excludedPartition.Range.First, excludedPartition.Range.Last, stopwatch.Elapsed.TotalMilliseconds);
                    await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, partition.Id, descriptionPartitionSplit, cancellation);
                }
                else
                {
                    await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, partition.Id, EngineResources.EventPartitionTooSmallToBeSplit, cancellation);
                }
            }
            else
            {
                await this.Dispatcher.DispatchEventAsync(EventSeverity.Information, job.Id, partition.Id, EngineResources.EventCouldNotSplitPartition, cancellation);
            }

            return result;
        }

        /// <summary>
        ///   Marks a job entry as started.
        /// </summary>
        /// <param name="id">
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
        private async Task<Job> MarkJobAsStartedAsync(string id, CancellationToken cancellation)
        {
            DateTime timestamp = this.Clock.GetDateTime();
            Job result = await this.Store.MarkJobAsStartedAsync(id, timestamp, cancellation);

            return result;
        }

        /// <summary>
        ///   Marks a job entry as completed.
        /// </summary>
        /// <param name="id">
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
        private async Task<Job> MarkJobAsCompletedAsync(string id, CancellationToken cancellation)
        {
            DateTime timestamp = this.Clock.GetDateTime();
            Job result = await this.Store.MarkJobAsCompletedAsync(id, timestamp, cancellation);

            return result;
        }
    }
}
