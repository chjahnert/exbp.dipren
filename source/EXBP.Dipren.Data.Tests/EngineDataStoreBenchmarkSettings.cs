
using System.Diagnostics;


namespace EXBP.Dipren.Data.Tests
{
    [DebuggerDisplay("Processing Nodes = {ProcessingNodes}, Dataset Size = {DatasetSize}, Batch Size = {BatchSize}, Processing Timeout = {ProcessingTimeout}, Polling Interval = {PollingInterval}")]
    public class EngineDataStoreBenchmarkSettings
    {
        private const int DEFAULT_PROCESSING_NODES = 13;
        private const int DEFAULT_DATASET_SIZE = 100_000;
        private const int DEFAULT_BATCH_SIZE = 9;
        private const int DEFAULT_PROCESSING_TIMEOUT = 1000;
        private const int DEFAULT_POLLING_INTERVAL = 250;
        private const int DEFAULT_REPORTING_INTERVAL = 100;
        private const int DEFAULT_BATCH_PROCESSING_DURATION = 0;


        public static EngineDataStoreBenchmarkSettings Tiny { get; } = new EngineDataStoreBenchmarkSettings
        {
            ProcessingNodes = 3,
            DatasetSize = 1000
        };

        public static EngineDataStoreBenchmarkSettings Small { get; } = new EngineDataStoreBenchmarkSettings
        {
            ProcessingNodes = 7,
            DatasetSize = 10_000
        };

        public static EngineDataStoreBenchmarkSettings Medium { get; } = new EngineDataStoreBenchmarkSettings();

        public static EngineDataStoreBenchmarkSettings Large { get; } = new EngineDataStoreBenchmarkSettings
        {
            ProcessingNodes = 23,
            DatasetSize = 1_000_000
        };

        public static EngineDataStoreBenchmarkSettings Huge { get; } = new EngineDataStoreBenchmarkSettings
        {
            ProcessingNodes = 47,
            DatasetSize = 10_000_000
        };


        public int ProcessingNodes { get; init; } = DEFAULT_PROCESSING_NODES;

        public int DatasetSize { get; init; } = DEFAULT_DATASET_SIZE;

        public int BatchSize { get; init; } = DEFAULT_BATCH_SIZE;

        public TimeSpan ProcessingTimeout { get; init; } = TimeSpan.FromMilliseconds(DEFAULT_PROCESSING_TIMEOUT);

        public TimeSpan PollingInterval { get; init; } = TimeSpan.FromMilliseconds(DEFAULT_POLLING_INTERVAL);

        public TimeSpan ReportingInterval { get; init; } = TimeSpan.FromMilliseconds(DEFAULT_REPORTING_INTERVAL);

        public TimeSpan BatchProcessingDuration { get; init; } = TimeSpan.FromMilliseconds(DEFAULT_BATCH_PROCESSING_DURATION);
    }
}
