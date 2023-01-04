
namespace EXBP.Dipren.Data.Tests
{
    public class EngineDataStoreBenchmarkSettings
    {
        private const int DEFAULT_PROCESSING_NODES = 13;
        private const int DEFAULT_DATASET_SIZE = 100000;
        private const int DEFAULT_PROCESSING_TIMEOUT = 1000;
        private const int DEFAULT_POLLING_INTERVAL = 100;
        private const int DEFAULT_REPORTING_INTERVAL = 20;
        private const int DEFAULT_BATCH_PROCESSING_DURATION = 0;


        public static EngineDataStoreBenchmarkSettings Default { get; } = new EngineDataStoreBenchmarkSettings();

        public int ProcessingNodes { get; init; } = DEFAULT_PROCESSING_NODES;

        public int DatasetsSize { get; init; } = DEFAULT_DATASET_SIZE;

        public TimeSpan ProcessingTimeout { get; init; } = TimeSpan.FromMilliseconds(DEFAULT_PROCESSING_TIMEOUT);

        public TimeSpan PollingInterval { get; init; } = TimeSpan.FromMilliseconds(DEFAULT_POLLING_INTERVAL);

        public TimeSpan ReportingInterval { get; init; } = TimeSpan.FromMilliseconds(DEFAULT_REPORTING_INTERVAL);

        public TimeSpan BatchProcessingDuration { get; init; } = TimeSpan.FromMilliseconds(DEFAULT_BATCH_PROCESSING_DURATION);
    }
}
