
using System.Diagnostics;
using System.Globalization;

using EXBP.Dipren.Data;
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements a processing engine that can iterate over a large set of ordered items in a distributed fashion.
    /// </summary>
    public class Engine : Node
    {
        private readonly Configuration _configuration;
        private readonly IEngineMetrics _metrics;
        private readonly Events _events;


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
        ///   The configuration settings to use; or <see langword="null"/> to use the default configuration settings.
        /// </param>
        /// <param name="clock">
        ///   A <see cref="ITimestampProvider"/> that can be used to generate timestamp values; or
        ///   <see langword="null"/> to use a <see cref="UtcTimestampProvider"/> instance.
        /// </param>
        /// <param name="handler">
        ///   The <see cref="IEventHandler"/> object to use to emit event notifications; or <see langword="null"/> to
        ///   discard event notifications.
        /// </param>
        /// <param name="metrics">
        ///   The <see cref="IEngineMetrics"/> object to use to collect performance metrics; or <see langword="null"/>
        ///   to use <see cref="OpenTelemetryEngineMetrics"/> instance.
        /// </param>
        internal Engine(IEngineDataStore store, Configuration configuration = null, ITimestampProvider clock = null, IEventHandler handler = null, IEngineMetrics metrics = null) : base(NodeType.Engine, store, clock, handler)
        {
            this._configuration = (configuration ?? new Configuration());
            this._metrics = (metrics ?? OpenTelemetryEngineMetrics.Instance);
            this._events = new Events(this.Dispatcher);
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="Engine"/> class.
        /// </summary>
        /// <param name="store">
        ///   The <see cref="IEngineDataStore"/> to use.
        /// </param>
        /// <param name="configuration">
        ///   The configuration settings to use; or <see langword="null"/> to use the default configuration settings.
        /// </param>
        /// <param name="handler">
        ///   The <see cref="IEventHandler"/> object to use to emit event notifications; or <see langword="null"/> to
        ///   discard event notifications.
        /// </param>
        /// <param name="metrics">
        ///   The <see cref="IEngineMetrics"/> object to use to collect performance metrics; or <see langword="null"/>
        ///   to use <see cref="OpenTelemetryEngineMetrics"/> instance.
        /// </param>
        public Engine(IEngineDataStore store, Configuration configuration = null, IEventHandler handler = null, IEngineMetrics metrics = null) : base(NodeType.Engine, store, null, handler)
        {
            this._configuration = (configuration ?? new Configuration());
            this._metrics = (metrics ?? OpenTelemetryEngineMetrics.Instance);
            this._events = new Events(this.Dispatcher);
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
            => await this.RunImpAsync(job, wait, cancellation).ConfigureAwait(false);


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
        private async Task RunImpAsync<TKey, TItem>(Job<TKey, TItem> job, bool wait, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(job, nameof(job));

            this._events.RaiseJobStarted(job.Id);
            this._metrics.RegisterEngineState(this.Id, job.Id, EngineState.Ready);

            try
            {
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
                    this._events.RaiseJobNotStarted(job.Id);

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
                    this._events.RaiseWaitingForJobToBeReady(job.Id);

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

                if (persisted.State == JobState.Completed || persisted.State == JobState.Failed)
                {
                    this._events.RaiseJobCompleted(job.Id);
                }
                else
                {
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
                            this._metrics.RegisterEngineState(this.Id, job.Id, EngineState.Processing);

                            try
                            {
                                await this.ProcessPartitionAsync(job, partition, settings, cancellation);
                            }
                            catch (LockException)
                            {
                                //
                                // The partition being processed was taken by another processing node.
                                //

                                this._events.RaisePartitionTaken(job.Id, partition.Id);
                            }
                            finally
                            {
                                this._metrics.RegisterEngineState(this.Id, job.Id, EngineState.Ready);
                            }
                        }
                        else
                        {
                            long incomplete = await this.Store.CountIncompletePartitionsAsync(job.Id, cancellation);

                            if (incomplete == 0L)
                            {
                                persisted = await this.MarkJobAsCompletedAsync(persisted.Id, cancellation);

                                this._events.RaiseJobCompleted(job.Id);
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
            }
            catch (Exception ex)
            {
                this._events.RaiseProcessingFailed(job.Id, ex);

                throw;
            }
            finally
            {
                this._metrics.RegisterEngineState(this.Id, job.Id, EngineState.Stopped);
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

            //
            // 1. Fetch the next batch of items to be processed from the data source.
            // 2. Process the batch.
            // 3. Try updating the partition with the progress made while reading the split request flag.
            //    The partition can only be updated if it is still owned by the current processing node.
            // 4. If a split was requested, create a new partition and save it along with the current partition in an
            //    atomic fashion. Must not update and insert if the current node no longer owns the partition.
            // 5. Repeat from step 1 until completed.
            //

            this._events.RaiseProcessingPartition(job.Id, partition.Id);

            Stopwatch iteration = Stopwatch.StartNew();
            MovingAverage throughputs = new MovingAverage(1024);

            while (partition.IsCompleted == false)
            {
                TKey first = ((partition.Processed == 0L) ? partition.Range.First : partition.Position);
                Range<TKey> range = new Range<TKey>(first, partition.Range.Last, partition.Range.IsInclusive);
                int skip = ((partition.Processed == 0L) ? 0 : 1);

                this._events.RaiseRequestingNextBatch(job.Id, partition.Id, settings.BatchSize, job.Serializer, range.First, range.Last, skip);

                Stopwatch stopwatch = Stopwatch.StartNew();

                IEnumerable<KeyValuePair<TKey, TItem>> batch = await job.Source.GetNextBatchAsync(range, skip, settings.BatchSize, cancellation);

                stopwatch.Stop();

                long count = batch.Count();

                this._metrics.RegisterBatchRetrieved(this.Id, job.Id, partition.Id, count, true, stopwatch.Elapsed);
                this._events.RaiseBatchRetrieved(job.Id, partition.Id, count, stopwatch.Elapsed);

                if (count > 0L)
                {
                    IEnumerable<TItem> items = batch.Select(kvp => kvp.Value);

                    bool succeeded = true;

                    stopwatch.Restart();

                    try
                    {
                        await job.Processor.ProcessAsync(items, cancellation);
                    }
                    catch (Exception ex)
                    {
                        this._metrics.RegisterBatchProcessed(this.Id, job.Id, partition.Id, count, false, stopwatch.Elapsed);
                        this._events.RaiseBatchProcessingFailed(job.Id, partition.Id, ex, count, job.Serializer, batch.First().Key, batch.Last().Key);

                        succeeded = false;
                    }

                    stopwatch.Stop();

                    if (succeeded == true)
                    {
                        this._metrics.RegisterBatchProcessed(this.Id, job.Id, partition.Id, count, true, stopwatch.Elapsed);
                        this._events.RaiseBatchProcessed(job.Id, partition.Id, count, stopwatch.Elapsed);

                        if (stopwatch.Elapsed >= settings.Timeout)
                        {
                            this._events.RaiseTimeoutValueTooLow(job.Id, partition.Id);
                        }
                    }
                }

                bool completed = (count < settings.BatchSize);
                TKey position = ((count == 0L) ? partition.Position : batch.Last().Key);
                DateTime timestamp = this.Clock.GetCurrentTimestamp();
                long processed = (partition.Processed + count);
                long remaining = ((partition.Remaining >= count) ? (partition.Remaining - count) : 0L);

                double seconds = iteration.Elapsed.TotalSeconds;

                iteration.Restart();

                if (seconds > 0.0)
                {
                    throughputs.Add(count / seconds);
                }

                partition = await this.ReportProgressAsync(job, partition, timestamp, position, processed, remaining, completed, throughputs.Average, cancellation);

                if (partition.IsSplitRequested == true)
                {
                    partition = await this.SplitPartitionAsync(job, partition, settings, cancellation);
                }
            }

            this._metrics.RegisterPartitionCompleted(this.Id, job.Id, partition.Id);
            this._events.RaisePartitionCompleted(job.Id, partition.Id);
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

            this._events.RaiseTryingToAcquirePartition(job.Id);

            DateTime now = this.Clock.GetCurrentTimestamp();
            DateTime cut = (now - settings.Timeout - settings.ClockDrift);

            Stopwatch stopwatch = Stopwatch.StartNew();

            Partition acquired = await this.Store.TryAcquirePartitionAsync(job.Id, this.Id, now, cut, cancellation);

            stopwatch.Stop();

            this._metrics.RegisterTryAcquirePartition(this.Id, job.Id, (acquired != null), stopwatch.Elapsed);

            Partition<TKey> result = null;

            if (acquired != null)
            {
                this._events.RaisePartitionAcquired(job.Id, acquired.Id);

                result = acquired.ToPartition(job.Serializer);
            }
            else
            {
                this._events.RaisePartitionNotAcquired(job.Id);

                stopwatch.Restart();

                bool pending = await this.Store.IsSplitRequestPendingAsync(job.Id, this.Id, cancellation);

                stopwatch.Stop();

                this._metrics.RegisterIsSplitRequestPending(this.Id, job.Id, stopwatch.Elapsed);

                if (pending == false)
                {
                    this._events.RaiseRequestingSplit(job.Id);

                    stopwatch.Restart();

                    bool succeeded = await this.Store.TryRequestSplitAsync(job.Id, this.Id, cut, cancellation);

                    stopwatch.Stop();

                    if (succeeded == true)
                    {
                        this._events.RaiseSplitRequestSucceeded(job.Id);
                    }
                    else
                    {
                        this._events.RaiseSplitRequestFailed(job.Id);
                    }

                    this._metrics.RegisterTryRequestSplit(this.Id, job.Id, succeeded, stopwatch.Elapsed);
                }
                else
                {
                    this._events.RaiseSplitAlreadyRequested(job.Id);
                }
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
        /// <param name="processed">
        ///   The total number of items processed in this partition.
        /// </param>
        /// <param name="remaining">
        ///   The total number of items remaining in this partition.
        /// </param>
        /// <param name="completed">
        ///   <see langword="true"/> if the partition is completed; otherwise, <see langword="false"/>.
        /// </param>
        /// <param name="throughput">
        ///   The average number of items processed per second.
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
        private async Task<Partition<TKey>> ReportProgressAsync<TKey, TItem>(Job<TKey, TItem> job, Partition<TKey> partition, DateTime timestamp, TKey position, long processed, long remaining, bool completed, double throughput, CancellationToken cancellation)
        {
            Debug.Assert(job != null);
            Debug.Assert(partition != null);
            Debug.Assert(partition.Owner == this.Id);
            Debug.Assert(position != null);
            Debug.Assert(processed >= 0L);
            Debug.Assert(remaining >= 0L);

            Stopwatch stopwatch = Stopwatch.StartNew();

            string serializedPosition = job.Serializer.Serialize(position);

            long adjustedRemaining = (completed == false) ? remaining : 0L;
            double adjustedThroughput = (completed == false) ? throughput : 0.0;

            Partition updated = await this.Store.ReportProgressAsync(partition.Id, this.Id, timestamp, serializedPosition, processed, adjustedRemaining, completed, adjustedThroughput, cancellation);

            Partition<TKey> result = updated.ToPartition(job.Serializer);

            stopwatch.Stop();

            this._metrics.RegisterReportProgress(this.Id, job.Id, partition.Id, stopwatch.Elapsed);

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

            this._events.RaiseSplitRequested(job.Id, partition.Id);

            Range<TKey> remainingKeyRange = partition.GetRemainingKeyRange();
            RangePartitioningResult<TKey> ranges = await job.Partitioner.SplitAsync(remainingKeyRange, cancellation);

            Partition<TKey> result = partition;

            if (ranges?.Success == true)
            {
                if (ranges.Created.Count > 1)
                {
                    throw new NotSupportedException(EngineResources.RangeSplitIntoTooManyRanges);
                }

                Stopwatch stopwatch = Stopwatch.StartNew();

                Range<TKey> updated = ranges.Updated;
                Range<TKey> created = ranges.Created.First();

                Task<long> createdKeyRangeSize = job.Source.EstimateRangeSizeAsync(created, cancellation);
                Task<long> updatedKeyRangeSize = job.Source.EstimateRangeSizeAsync(updated, cancellation);

                if ((await createdKeyRangeSize >= settings.BatchSize) && (await updatedKeyRangeSize >= settings.BatchSize))
                {
                    DateTime timestamp = this.Clock.GetCurrentTimestamp();

                    Partition<TKey> updatedPartition = new Partition<TKey>(partition.Id, partition.JobId, partition.Owner, partition.Created, timestamp, updated, partition.Position, partition.Processed, (await updatedKeyRangeSize - 1), false, 0.0, null);
                    Partition updatedEntry = updatedPartition.ToEntry(job.Serializer);

                    Guid id = Guid.NewGuid();
                    Partition<TKey> excludedPartition = new Partition<TKey>(id, partition.JobId, null, timestamp, timestamp, created, default, 0L, await createdKeyRangeSize, false, 0.0, null);
                    Partition excludedEntry = excludedPartition.ToEntry(job.Serializer);

                    await this.Store.InsertSplitPartitionAsync(updatedEntry, excludedEntry, cancellation);

                    result = updatedPartition;

                    this._metrics.RegisterPartitionCreated(this.Id, job.Id, partition.Id);
                    this._events.RaisePartitionSplit(job.Id, partition.Id, job.Serializer, updatedPartition.Range.First, updatedPartition.Range.Last, excludedPartition.Id, excludedPartition.Range.First, excludedPartition.Range.Last, stopwatch.Elapsed);
                }
                else
                {
                    this._events.RaisePartitionTooSmallToBeSplit(job.Id, partition.Id);
                }
            }
            else
            {
                this._events.RaiseCouldNotSplitPartition(job.Id, partition.Id);
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
            DateTime timestamp = this.Clock.GetCurrentTimestamp();
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
            DateTime timestamp = this.Clock.GetCurrentTimestamp();
            Job result = await this.Store.MarkJobAsCompletedAsync(id, timestamp, cancellation);

            return result;
        }


        /// <summary>
        ///   Implements methods for raising <see cref="Engine"/> events.
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
            ///   Raises the event when a distributed processing job is started.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            internal void RaiseJobStarted(string jobId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, EngineResources.EventJobStarted);
            }

            /// <summary>
            ///   Raises the event when a distributed processing could not be started.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            internal void RaiseJobNotStarted(string jobId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, EngineResources.EventJobNotScheduled);
            }

            /// <summary>
            ///   Raises the event when the <see cref="Engine"/> is waiting for the distributed processing job to be
            ///   ready.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            internal void RaiseWaitingForJobToBeReady(string jobId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Debug, jobId, EngineResources.EventWaitingForJobToBeReady);
            }

            /// <summary>
            ///   Raises the event when a distributed processing job completes.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            internal void RaiseJobCompleted(string jobId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, EngineResources.EventJobCompleted);
            }

            /// <summary>
            ///   Raises the event when another processing node takes ownership of the partition.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            internal void RaisePartitionTaken(string jobId, Guid partitionId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, partitionId, EngineResources.EventPartitionTaken);
            }

            /// <summary>
            ///   Raises the event when a distributed processing job failed.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="exception">
            ///   The exception that caused the job to fail.
            /// </param>
            internal void RaiseProcessingFailed(string jobId, Exception exception)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(exception != null);

                this._dispatcher.DispatchEvent(EventSeverity.Error, jobId, EngineResources.EventProcessingFailed, exception);
            }

            /// <summary>
            ///   Raises the event when processing of a partition is started.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            internal void RaiseProcessingPartition(string jobId, Guid partitionId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, partitionId, EngineResources.EventProcessingPartition);
            }

            /// <summary>
            ///   Raises the event when a new batch is requested during processing.
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
            /// <param name="batchSize">
            ///   The size of the batch requested.
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
            /// <param name="skip">
            ///   The number of keys to skip in the current range.
            /// </param>
            internal void RaiseRequestingNextBatch<TKey>(string jobId, Guid partitionId, int batchSize, IKeySerializer<TKey> serializer, TKey first, TKey last, int skip)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(batchSize >= 0);
                Debug.Assert(serializer != null);
                Debug.Assert(first != null);
                Debug.Assert(last != null);
                Debug.Assert(skip >= 0);

                string serializedFirst = serializer.Serialize(first);
                string serializedLast = serializer.Serialize(last);
                string message = string.Format(CultureInfo.InvariantCulture, EngineResources.EventRequestingNextBatch, batchSize, serializedFirst, serializedLast, skip);

                this._dispatcher.DispatchEvent(EventSeverity.Debug, jobId, partitionId, message);
            }

            /// <summary>
            ///   Raises the event when a batch of items was retrieved.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            /// <param name="count">
            ///   The number of items in the batch.
            /// </param>
            /// <param name="duration">
            ///   The time it took to retrieve the batch of items.
            /// </param>
            internal void RaiseBatchRetrieved(string jobId, Guid partitionId, long count, TimeSpan duration)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(count >= 0);
                Debug.Assert(duration >= TimeSpan.Zero);

                string message = string.Format(CultureInfo.InvariantCulture, EngineResources.EventBatchRetrieved, count, duration.TotalMilliseconds);

                this._dispatcher.DispatchEvent(EventSeverity.Debug, jobId, partitionId, message);
            }

            /// <summary>
            ///   Raises the event when processing a batch of items failed.
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
            /// <param name="exception">
            ///   The exception that caused the processing to fail.
            /// </param>
            /// <param name="batchSize">
            ///   The size of the batch.
            /// </param>
            /// <param name="serializer">
            ///   The <see cref="IKeySerializer{TKey}"/> of type <typeparamref name="TKey"/> that is used to serialize
            ///   keys.
            /// </param>
            /// <param name="first">
            ///   The first key in the batch.
            /// </param>
            /// <param name="last">
            ///   The last key in the batch.
            /// </param>
            internal void RaiseBatchProcessingFailed<TKey>(string jobId, Guid partitionId, Exception exception, long batchSize, IKeySerializer<TKey> serializer, TKey first, TKey last)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(exception != null);
                Debug.Assert(batchSize > 0);
                Debug.Assert(serializer != null);
                Debug.Assert(first != null);
                Debug.Assert(last != null);

                string batch;

                if (batchSize > 1)
                {
                    string serializedFirst = serializer.Serialize(first);
                    string serializedLast = serializer.Serialize(last);

                    batch = string.Format(CultureInfo.InvariantCulture, EngineResources.BatchDescriptior, serializedFirst, serializedLast);
                }
                else
                {
                    batch = serializer.Serialize(first);
                }

                string message = string.Format(CultureInfo.InvariantCulture, EngineResources.EventBatchProcessingFailed, batch);

                this._dispatcher.DispatchEvent(EventSeverity.Warning, jobId, partitionId, message, exception);
            }

            /// <summary>
            ///   Raises the event when a batch of items was processed successfully.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            /// <param name="batchSize">
            ///   The number of items in the batch.
            /// </param>
            /// <param name="duration">
            ///   The time it took to process the batch of items.
            /// </param>
            internal void RaiseBatchProcessed(string jobId, Guid partitionId, long batchSize, TimeSpan duration)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(batchSize >= 0);
                Debug.Assert(duration >= TimeSpan.Zero);

                string message = string.Format(CultureInfo.InvariantCulture, EngineResources.EventBatchProcessed, batchSize, duration.TotalMilliseconds);

                this._dispatcher.DispatchEvent(EventSeverity.Debug, jobId, partitionId, message);
            }

            /// <summary>
            ///   Raises the event when the duration of processing a batch exceeded that configured timeout value.
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

                this._dispatcher.DispatchEvent(EventSeverity.Warning, jobId, partitionId, EngineResources.EventTimeoutValueTooLow);
            }

            /// <summary>
            ///   Raises the event when a partition is completed.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            internal void RaisePartitionCompleted(string jobId, Guid partitionId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, partitionId, EngineResources.EventPartitionCompleted);
            }

            /// <summary>
            ///   Raises the event when an <see cref="Engine"/> instance is attempting to acquire a partition.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            internal void RaiseTryingToAcquirePartition(string jobId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, EngineResources.EventTryingToAcquirePartition);
            }

            /// <summary>
            ///   Raises the event when an <see cref="Engine"/> instance acquired a partition.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            internal void RaisePartitionAcquired(string jobId, Guid partitionId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, partitionId, EngineResources.EventPartitionAcquired);
            }

            /// <summary>
            ///   Raises the event when an attempt to acquire a partition did not succeed.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            internal void RaisePartitionNotAcquired(string jobId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, EngineResources.EventPartitionNotAcquired);
            }

            /// <summary>
            ///   Raises the event when a <see cref="Engine"/> instance is about to request a partition to be split.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            internal void RaiseRequestingSplit(string jobId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, EngineResources.EventRequestingSplit);
            }

            /// <summary>
            ///   Raises the event when a <see cref="Engine"/> instance found a suitable partition and requested it to
            ///   be split.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            internal void RaiseSplitRequestSucceeded(string jobId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, EngineResources.EventSplitRequestSucceeded);
            }

            /// <summary>
            ///   Raises the event when a <see cref="Engine"/> instance could not find a suitable partition to be split.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            internal void RaiseSplitRequestFailed(string jobId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, EngineResources.EventSplitRequestFailed);
            }

            /// <summary>
            ///   Raises the event when a <see cref="Engine"/> instance did not request a partition to be split because
            ///   there is already a split request pending.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            internal void RaiseSplitAlreadyRequested(string jobId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Debug, jobId, EngineResources.EventSplitAlreadyRequested);
            }

            /// <summary>
            ///   Raises the event when an <see cref="Engine"/> instance receives a request to split the partition it
            ///   is currently processing.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            internal void RaiseSplitRequested(string jobId, Guid partitionId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, partitionId, EngineResources.EventSplitRequested);
            }

            /// <summary>
            ///   Raises the event when a partition was split successfully.
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
            /// <param name="updatedFirst">
            ///   The first key in the partition after it was split.
            /// </param>
            /// <param name="updatedLast">
            ///   The last key in the partition after it was split.
            /// </param>
            /// <param name="createdPartitionId">
            ///   The unique identifier of the partition created as a result of the split.
            /// </param>
            /// <param name="createdFirst">
            ///   The first key of the partition created as a result of the split.
            /// </param>
            /// <param name="createdLast">
            ///   The last key of the partition created as a result of the split.
            /// </param>
            /// <param name="duration">
            ///   The time it took to split the partition.
            /// </param>
            internal void RaisePartitionSplit<TKey>(string jobId, Guid partitionId, IKeySerializer<TKey> serializer, TKey updatedFirst, TKey updatedLast, Guid createdPartitionId, TKey createdFirst, TKey createdLast, TimeSpan duration)
            {
                Debug.Assert(jobId != null);
                Debug.Assert(serializer != null);
                Debug.Assert(updatedFirst != null);
                Debug.Assert(updatedLast != null);
                Debug.Assert(createdFirst != null);
                Debug.Assert(createdLast != null);
                Debug.Assert(duration >= TimeSpan.Zero);

                string serializedUpdatedFirst = serializer.Serialize(updatedFirst);
                string serializedUpdatedLast = serializer.Serialize(updatedLast);
                string serializedCreatedFirst = serializer.Serialize(createdFirst);
                string serializedCreatedLast = serializer.Serialize(createdLast);

                string message = String.Format(CultureInfo.InvariantCulture, EngineResources.EventPartitionSplit, serializedUpdatedFirst, serializedUpdatedLast, createdPartitionId, serializedCreatedFirst, serializedCreatedLast, duration.TotalMilliseconds);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, partitionId, message);
            }

            /// <summary>
            ///   Raises the event when a partition is too small to be split.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            internal void RaisePartitionTooSmallToBeSplit(string jobId, Guid partitionId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, partitionId, EngineResources.EventPartitionTooSmallToBeSplit);
            }

            /// <summary>
            ///   Raises the event when a partition could not be split.
            /// </summary>
            /// <param name="jobId">
            ///   The unique identifier of the distributed processing job.
            /// </param>
            /// <param name="partitionId">
            ///   The unique identifier of the partition.
            /// </param>
            internal void RaiseCouldNotSplitPartition(string jobId, Guid partitionId)
            {
                Debug.Assert(jobId != null);

                this._dispatcher.DispatchEvent(EventSeverity.Information, jobId, partitionId, EngineResources.EventCouldNotSplitPartition);
            }
        }
    }
}
