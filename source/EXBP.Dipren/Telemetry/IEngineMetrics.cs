
namespace EXBP.Dipren.Telemetry
{
    /// <summary>
    ///   Allows a type to implements a component that collects performance metrics for the <see cref="Engine"/> type.
    /// </summary>
    public interface IEngineMetrics
    {
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
        void RegisterEngineState(string nodeId, string jobId, EngineState state);

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
        void RegisterPartitionCreated(string nodeId, string jobId, Guid? partitionId, long count = 1L);

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
        void RegisterPartitionCompleted(string nodeId, string jobId, Guid? partitionId, long count = 1L);

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
        void RegisterBatchRetrieved(string nodeId, string jobId, Guid partitionId, long keys, bool success, TimeSpan duration);

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
        void RegisterBatchProcessed(string nodeId, string jobId, Guid partitionId, long keys, bool success, TimeSpan duration);

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
        void RegisterIsSplitRequestPending(string nodeId, string jobId, TimeSpan duration);

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
         void RegisterTryAcquirePartition(string nodeId, string jobId, bool success, TimeSpan duration);

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
        void RegisterTryRequestSplit(string nodeId, string jobId, bool success, TimeSpan duration);

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
        void RegisterReportProgress(string nodeId, string jobId, Guid partitionId, TimeSpan duration);
    }
}
