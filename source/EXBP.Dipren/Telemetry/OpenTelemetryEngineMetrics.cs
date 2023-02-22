
using System.Diagnostics;
using System.Diagnostics.Metrics;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Telemetry
{
    /// <summary>
    ///   Implements performance metrics for the <see cref="Engine"/> type using OpenTelemetry.
    /// </summary>
    public sealed class OpenTelemetryEngineMetrics : IEngineMetrics, IDisposable
    {
        private const string METER_NAME = "EXBP.Dipren";

        internal const string INSTRUMENT_NAME_KEYS_RETRIEVED = "keys-retrieved";
        internal const string INSTRUMENT_NAME_BATCHES_RETRIEVED = "batches-retrieved";
        internal const string INSTRUMENT_NAME_KEYS_COMPLETED = "keys-completed";
        internal const string INSTRUMENT_NAME_BATCHES_COMPLETED = "batches-completed";
        internal const string INSTRUMENT_NAME_PARTITIONS_CREATED = "partitions-created";
        internal const string INSTRUMENT_NAME_PARTITIONS_COMPLETED = "partitions-completed";

        internal const string INSTRUMENT_NAME_IS_SPLIT_REQUEST_PENDING = "is-split-request-pending";
        internal const string INSTRUMENT_NAME_TRY_ACQUIRE_PARTITION = "try-acquire-partition";
        internal const string INSTRUMENT_NAME_BATCH_RETRIEVAL = "batch-retrieval";
        internal const string INSTRUMENT_NAME_BATCH_PROCESSING = "batch-processing";
        internal const string INSTRUMENT_NAME_REPORT_PROGRESS = "report-progress";

        internal const string TAG_NAME_NODE = "node";
        internal const string TAG_NAME_JOB = "job";
        internal const string TAG_NAME_PARTITION = "partition";
        internal const string TAG_NAME_OUTCOME = "outcome";
        internal const string TAG_NAME_STATE = "state";

        internal const string TAG_VALUE_READY = "ready";
        internal const string TAG_VALUE_PROCESSING = "processing";
        internal const string TAG_VALUE_SUCCESS = "success";
        internal const string TAG_VALUE_FAILURE = "failure";

        private static readonly KeyValuePair<string, object> TagEngineStateReady = new KeyValuePair<string, object>(TAG_NAME_STATE, TAG_VALUE_READY);
        private static readonly KeyValuePair<string, object> TagEngineStateProcessing = new KeyValuePair<string, object>(TAG_NAME_STATE, TAG_VALUE_PROCESSING);


        private readonly Meter _meter;

        private readonly Counter<long> _keysRetrievedCounter;
        private readonly Counter<long> _keysCompletedCounter;
        private readonly Counter<long> _batchesRetrievedCounter;
        private readonly Counter<long> _batchesCompletedCounter;
        private readonly Counter<long> _partitionsCreatedCounter;
        private readonly Counter<long> _partitionsCompletedCounter;

        private readonly Histogram<double> _isSplitRequestPendingDuration;
        private readonly Histogram<double> _tryAcquirePartitionDuration;
        private readonly Histogram<double> _tryRequestSplitDuration;
        private readonly Histogram<double> _batchRetrievalDuration;
        private readonly Histogram<double> _batchProcessingDuration;
        private readonly Histogram<double> _reportProgressDuration;

        private readonly ObservableGauge<int> _enginesGauge;

        private readonly object _enginesSyncRoot = new object();
        private readonly Dictionary<string, EngineStateEntry> _engines = new Dictionary<string, EngineStateEntry>();


        /// <summary>
        ///   Gets the default instance of the <see cref="OpenTelemetryEngineMetrics"/> type.
        /// </summary>
        /// <value>
        ///   A <see cref="OpenTelemetryEngineMetrics"/> object that can be injected into a <see cref="Engine"/>
        ///   instance.
        /// </value>
        public static OpenTelemetryEngineMetrics Instance { get; } = new OpenTelemetryEngineMetrics();

        /// <summary>
        ///   Gets the name of the meter.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value containing the name of the meter.
        /// </value>
        public static string MeterName { get; } = METER_NAME;


        /// <summary>
        ///   Initializes a new instance of the <see cref="OpenTelemetryEngineMetrics"/> class.
        /// </summary>
        private OpenTelemetryEngineMetrics()
        {
            this._meter = new Meter(METER_NAME);

            this._keysRetrievedCounter = this._meter.CreateCounter<long>(INSTRUMENT_NAME_KEYS_RETRIEVED, OpenTelemetryEngineMetricsResources.UnitKeys, OpenTelemetryEngineMetricsResources.InstrumentDescriptionKeysRetrieved);
            this._keysCompletedCounter = this._meter.CreateCounter<long>(INSTRUMENT_NAME_KEYS_COMPLETED, OpenTelemetryEngineMetricsResources.UnitKeys, OpenTelemetryEngineMetricsResources.InstrumentDescriptionKeysCompleted);
            this._batchesRetrievedCounter = this._meter.CreateCounter<long>(INSTRUMENT_NAME_BATCHES_RETRIEVED, OpenTelemetryEngineMetricsResources.UnitBatches, OpenTelemetryEngineMetricsResources.InstrumentDescriptionBatchesRetrieved);
            this._batchesCompletedCounter = this._meter.CreateCounter<long>(INSTRUMENT_NAME_BATCHES_COMPLETED, OpenTelemetryEngineMetricsResources.UnitBatches, OpenTelemetryEngineMetricsResources.InstrumentDescriptionBatchesCompleted);
            this._partitionsCreatedCounter = this._meter.CreateCounter<long>(INSTRUMENT_NAME_PARTITIONS_CREATED, OpenTelemetryEngineMetricsResources.UnitPartitions, OpenTelemetryEngineMetricsResources.InstrumentDescriptionPartitionsCreated);
            this._partitionsCompletedCounter = this._meter.CreateCounter<long>(INSTRUMENT_NAME_PARTITIONS_COMPLETED, OpenTelemetryEngineMetricsResources.UnitPartitions, OpenTelemetryEngineMetricsResources.InstrumentDescriptionPartitionsCompleted);

            this._isSplitRequestPendingDuration = this._meter.CreateHistogram<double>(INSTRUMENT_NAME_IS_SPLIT_REQUEST_PENDING, OpenTelemetryEngineMetricsResources.UnitMilliseconds, OpenTelemetryEngineMetricsResources.InstrumentDescriptionIsSplitRequestPendingDuration);
            this._tryAcquirePartitionDuration = this._meter.CreateHistogram<double>(INSTRUMENT_NAME_TRY_ACQUIRE_PARTITION, OpenTelemetryEngineMetricsResources.UnitMilliseconds, OpenTelemetryEngineMetricsResources.InstrumentDescriptionTryAcquirePartitionDuration);
            this._tryRequestSplitDuration = this._meter.CreateHistogram<double>("try-request-split", OpenTelemetryEngineMetricsResources.UnitMilliseconds, OpenTelemetryEngineMetricsResources.InstrumentDescriptionTryRequestSplitDuration);
            this._batchRetrievalDuration = this._meter.CreateHistogram<double>(INSTRUMENT_NAME_BATCH_RETRIEVAL, OpenTelemetryEngineMetricsResources.UnitMilliseconds, OpenTelemetryEngineMetricsResources.InstrumentDescriptionBatchRetrievalDuration);
            this._batchProcessingDuration = this._meter.CreateHistogram<double>(INSTRUMENT_NAME_BATCH_PROCESSING, OpenTelemetryEngineMetricsResources.UnitMilliseconds, OpenTelemetryEngineMetricsResources.InstrumentDescriptionBatchProcessingDuration);
            this._reportProgressDuration = this._meter.CreateHistogram<double>(INSTRUMENT_NAME_REPORT_PROGRESS, OpenTelemetryEngineMetricsResources.UnitMilliseconds, OpenTelemetryEngineMetricsResources.InstrumentDescriptionReportProgressDuration);

            this._enginesGauge = this._meter.CreateObservableGauge("engines", this.GetEngineStateMeasurements, OpenTelemetryEngineMetricsResources.UnitEngines, OpenTelemetryEngineMetricsResources.InstrumentDescriptionEngines);
        }


        /// <summary>
        ///   Releases managed and unmanaged resources used by the current instance.
        /// </summary>
        public void Dispose()
        {
            this._meter.Dispose();
        }


        /// <summary>
        ///   Registers the state of an engine instance.
        /// </summary>
        /// <param name="nodeId">
        ///   The unique identifier of the processing node.
        /// </param>
        /// <param name="jobId">
        ///   The unique identifier of the distributed processing job.
        /// </param>
        /// <param name="state">
        ///   A <see cref="EngineState"/> value indicating the state of the engine.
        /// </param>
        public void RegisterEngineState(string nodeId, string jobId, EngineState state)
        {
            Assert.ArgumentIsNotNull(nodeId, nameof(nodeId));
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));
            Assert.ArgumentIsDefined(state, nameof(state));

            lock (this._enginesSyncRoot)
            {
                bool exists = this._engines.TryGetValue(nodeId, out EngineStateEntry entry);

                if (state != EngineState.Stopped)
                {
                    if (exists == false)
                    {
                        entry = new EngineStateEntry(nodeId, jobId, state);

                        this._engines.Add(nodeId, entry);
                    }
                    else
                    {
                        entry.State = state;
                    }
                }
                else
                {
                    if (exists == true)
                    {
                        this._engines.Remove(nodeId);
                    }
                }
            }
        }

        /// <summary>
        ///   Returns a snapshot of the current engine states.
        /// </summary>
        /// <returns>
        ///   An <see cref="IEnumerable{T}"/> of <see cref="Measurement{T}"/> values containing the measurements.
        /// </returns>
        private IEnumerable<Measurement<int>> GetEngineStateMeasurements()
        {
            List<Measurement<int>> result = new List<Measurement<int>>();

            lock (this._enginesSyncRoot)
            {
                IEnumerable<IGrouping<string, EngineStateEntry>> groups = this._engines.Values.GroupBy(entry => entry.JobId);

                foreach(IGrouping<string, EngineStateEntry> group in groups)
                {
                    KeyValuePair<string, object> tagJob = new KeyValuePair<string, object>(TAG_NAME_JOB, group.Key);

                    Measurement<int> enginesReady = new Measurement<int>(group.Count(entry => entry.State == EngineState.Ready), TagEngineStateReady, tagJob);
                    Measurement<int> enginesProcessing = new Measurement<int>(group.Count(entry => entry.State == EngineState.Processing), TagEngineStateProcessing, tagJob);

                    result.Add(enginesReady);
                    result.Add(enginesProcessing);
                }
            }

            return result;
        }

        /// <summary>
        ///   Registers the event when a new partition is created.
        /// </summary>
        /// <param name="nodeId">
        ///   The unique identifier of the processing node on which the measurements were taken.
        /// </param>
        /// <param name="jobId">
        ///   The unique identifier of the job the measurements are related to.
        /// </param>
        /// <param name="partitionId">
        ///   The unique identifier of the partition the measurements are related to.
        /// </param>
        /// <param name="count">
        ///   The number of partitions created.
        /// </param>
        public void RegisterPartitionCreated(string nodeId, string jobId, Guid? partitionId, long count = 1L)
        {
            Assert.ArgumentIsNotNull(nodeId, nameof(nodeId));
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));

            TagList tags = this.CreateTags(nodeId, jobId, partitionId);

            this._partitionsCreatedCounter.Add(count, tags);
        }

        /// <summary>
        ///   Registers the event when a partition is completed.
        /// </summary>
        /// <param name="nodeId">
        ///   The unique identifier of the processing node on which the measurements were taken.
        /// </param>
        /// <param name="jobId">
        ///   The unique identifier of the job the measurements are related to.
        /// </param>
        /// <param name="partitionId">
        ///   The unique identifier of the partition the measurements are related to.
        /// </param>
        /// <param name="count">
        ///   The number of partitions created.
        /// </param>
        public void RegisterPartitionCompleted(string nodeId, string jobId, Guid? partitionId, long count = 1L)
        {
            Assert.ArgumentIsNotNull(nodeId, nameof(nodeId));
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));

            TagList tags = this.CreateTags(nodeId, jobId, partitionId);

            this._partitionsCompletedCounter.Add(count, tags);
        }

        /// <summary>
        ///    Registers the performance metrics after a batch of items was retrieved.
        /// </summary>
        /// <param name="nodeId">
        ///   The unique identifier of the processing node on which the measurements were taken.
        /// </param>
        /// <param name="jobId">
        ///   The unique identifier of the job the measurements are related to.
        /// </param>
        /// <param name="partitionId">
        ///   The unique identifier of the partition the measurements are related to.
        /// </param>
        /// <param name="keys">
        ///   The number of keys in the batch.
        /// </param>
        /// <param name="success">
        ///   A flag that indicates the outcome of the operation.
        /// </param>
        /// <param name="duration">
        ///   The duration of the operation.
        /// </param>
        public void RegisterBatchRetrieved(string nodeId, string jobId, Guid partitionId, long keys, bool success, TimeSpan duration)
        {
            Assert.ArgumentIsNotNull(nodeId, nameof(nodeId));
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));
            Assert.ArgumentIsGreaterOrEqual(keys, 0L, nameof(keys));
            Assert.ArgumentIsGreaterOrEqual(duration, TimeSpan.Zero, nameof(duration));

            TagList tags = this.CreateTags(nodeId, jobId, partitionId, (success ? OperationOutcome.Success : OperationOutcome.Failure));

            this._keysRetrievedCounter.Add(keys, tags);
            this._batchesRetrievedCounter.Add(1L, tags);
            this._batchRetrievalDuration.Record(duration.TotalMilliseconds, tags);
        }

        /// <summary>
        ///    Registers the performance metrics after a batch of items was processed.
        /// </summary>
        /// <param name="nodeId">
        ///   The unique identifier of the processing node on which the measurements were taken.
        /// </param>
        /// <param name="jobId">
        ///   The unique identifier of the job the measurements are related to.
        /// </param>
        /// <param name="partitionId">
        ///   The unique identifier of the partition the measurements are related to.
        /// </param>
        /// <param name="keys">
        ///   The number of keys in the batch.
        /// </param>
        /// <param name="success">
        ///   A flag that indicates the outcome of the operation.
        /// </param>
        /// <param name="duration">
        ///   The duration of the operation.
        /// </param>
        public void RegisterBatchProcessed(string nodeId, string jobId, Guid partitionId, long keys, bool success, TimeSpan duration)
        {
            Assert.ArgumentIsNotNull(nodeId, nameof(nodeId));
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));
            Assert.ArgumentIsGreaterOrEqual(keys, 0, nameof(keys));
            Assert.ArgumentIsGreaterOrEqual(duration, TimeSpan.Zero, nameof(duration));

            TagList tags = this.CreateTags(nodeId, jobId, partitionId, (success ? OperationOutcome.Success : OperationOutcome.Failure));

            this._keysCompletedCounter.Add(keys, tags);
            this._batchesCompletedCounter.Add(1L, tags);
            this._batchProcessingDuration.Record(duration.TotalMilliseconds, tags);
        }

        /// <summary>
        ///    Registers the performance metrics of checking if a split request is already pending.
        /// </summary>
        /// <param name="nodeId">
        ///   The unique identifier of the processing node on which the measurements were taken.
        /// </param>
        /// <param name="jobId">
        ///   The unique identifier of the job the measurements are related to.
        /// </param>
        /// <param name="duration">
        ///   The duration of the operation.
        /// </param>
        public void RegisterIsSplitRequestPending(string nodeId, string jobId, TimeSpan duration)
        {
            Assert.ArgumentIsNotNull(nodeId, nameof(nodeId));
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));
            Assert.ArgumentIsGreaterOrEqual(duration, TimeSpan.Zero, nameof(duration));

            TagList tags = this.CreateTags(nodeId, jobId, null);

            this._isSplitRequestPendingDuration.Record(duration.TotalMilliseconds, tags);
        }

        /// <summary>
        ///    Registers the performance metrics of checking if a split request is already pending.
        /// </summary>
        /// <param name="nodeId">
        ///   The unique identifier of the processing node on which the measurements were taken.
        /// </param>
        /// <param name="jobId">
        ///   The unique identifier of the job the measurements are related to.
        /// </param>
        /// <param name="success">
        ///   A flag that indicates the outcome of the operation.
        /// </param>
        /// <param name="duration">
        ///   The duration of the operation.
        /// </param>
        public void RegisterTryAcquirePartition(string nodeId, string jobId, bool success, TimeSpan duration)
        {
            Assert.ArgumentIsNotNull(nodeId, nameof(nodeId));
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));
            Assert.ArgumentIsGreaterOrEqual(duration, TimeSpan.Zero, nameof(duration));

            TagList tags = this.CreateTags(nodeId, jobId, null, (success ? OperationOutcome.Success : OperationOutcome.Failure));

            this._tryAcquirePartitionDuration.Record(duration.TotalMilliseconds, tags);
        }

        /// <summary>
        ///    Registers the performance metrics of checking if a split request is already pending.
        /// </summary>
        /// <param name="nodeId">
        ///   The unique identifier of the processing node on which the measurements were taken.
        /// </param>
        /// <param name="jobId">
        ///   The unique identifier of the job the measurements are related to.
        /// </param>
        /// <param name="success">
        ///   A flag that indicates the outcome of the operation.
        /// </param>
        /// <param name="duration">
        ///   The duration of the operation.
        /// </param>
        public void RegisterTryRequestSplit(string nodeId, string jobId, bool success, TimeSpan duration)
        {
            Assert.ArgumentIsNotNull(nodeId, nameof(nodeId));
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));
            Assert.ArgumentIsGreaterOrEqual(duration, TimeSpan.Zero, nameof(duration));

            TagList tags = this.CreateTags(nodeId, jobId, null, (success ? OperationOutcome.Success : OperationOutcome.Failure));

            this._tryRequestSplitDuration.Record(duration.TotalMilliseconds, tags);
        }

        /// <summary>
        ///    Registers the performance metrics of checking if a split request is already pending.
        /// </summary>
        /// <param name="nodeId">
        ///   The unique identifier of the processing node on which the measurements were taken.
        /// </param>
        /// <param name="jobId">
        ///   The unique identifier of the job the measurements are related to.
        /// </param>
        /// <param name="partitionId">
        ///   The unique identifier of the partition the measurements are related to.
        /// </param>
        /// <param name="duration">
        ///   The duration of the operation.
        /// </param>
        public void RegisterReportProgress(string nodeId, string jobId, Guid partitionId, TimeSpan duration)
        {
            Assert.ArgumentIsNotNull(nodeId, nameof(nodeId));
            Assert.ArgumentIsNotNull(jobId, nameof(jobId));
            Assert.ArgumentIsGreaterOrEqual(duration, TimeSpan.Zero, nameof(duration));

            TagList tags = this.CreateTags(nodeId, jobId, partitionId);

            this._reportProgressDuration.Record(duration.TotalMilliseconds, tags);
        }


        /// <summary>
        ///   Creates a tag list from the provided attributes.
        /// </summary>
        /// <param name="nodeId">
        ///   The unique identifier of the processing node.
        /// </param>
        /// <param name="jobId">
        ///   The unique identifier of the distributed processing job; or <see langword="null"/> if not available.
        /// </param>
        /// <param name="partitionId">
        ///   The unique identifier if the partition; or <see langword="null"/> if not available.
        /// </param>
        /// <param name="outcome">
        ///   A <see cref="OperationOutcome"/> value indicating the outcome of the operation; or <see langword="null"/>
        ///   if not relevant.
        /// </param>
        /// <returns>
        ///   A <see cref="TagList"/> object containing the tags for the provided measurement attributes.
        /// </returns>
        private TagList CreateTags(string nodeId, string jobId = null, Guid? partitionId = null, OperationOutcome? outcome = null)
        {
            Debug.Assert(nodeId != null);
            Debug.Assert(jobId != null);

            TagList result = new TagList
            {
                { TAG_NAME_NODE, nodeId }
            };

            if (jobId != null)
            {
                result.Add(TAG_NAME_JOB, jobId);
            }

            if (partitionId != null)
            {
                string value = partitionId.Value.ToString("d");

                result.Add(TAG_NAME_PARTITION, value);
            }

            if (outcome != null)
            {
                Assert.ArgumentIsDefined(outcome.Value, nameof(outcome));

                string value = null;

                switch (outcome.Value)
                {
                    case OperationOutcome.Success:
                        value = TAG_VALUE_SUCCESS;
                        break;

                    case OperationOutcome.Failure:
                        value = TAG_VALUE_FAILURE;
                        break;
                }

                result.Add(TAG_NAME_OUTCOME, value);
                ;
            }

            return result;
        }

        private enum OperationOutcome
        {
            Success,
            Failure
        }

        private class EngineStateEntry
        {
            public string NodeId { get; }

            public string JobId { get; }

            public EngineState State { get; set; }


            public EngineStateEntry(string nodeId, string jobId, EngineState state)
            {
                Debug.Assert(nodeId != null);
                Debug.Assert(jobId != null);

                this.NodeId = nodeId;
                this.JobId = jobId;
                this.State = state;
            }
        }
    }
}
