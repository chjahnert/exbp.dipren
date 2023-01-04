
using EXBP.Dipren.Telemetry;


namespace EXBP.Dipren.Data.Tests
{
    public class EngineDataStoreBenchmarkResult
    {
        public string Id { get; init; }

        public long Count { get; init; }

        public TimeSpan Duration { get; init; }

        public IEnumerable<StatusReport> Snapshots { get; init; }

        public IEnumerable<EventDescriptor> Events { get; init; }
    }
}
