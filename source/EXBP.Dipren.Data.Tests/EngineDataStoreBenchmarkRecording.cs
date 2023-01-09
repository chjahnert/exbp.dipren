
using System.Diagnostics;

using EXBP.Dipren.Telemetry;


namespace EXBP.Dipren.Data.Tests
{
    [DebuggerDisplay("Id = {Id}, Processed = {Processed}, Missed = {Missed}, Duration = {Duration}, Errors = {Errors}")]
    public class EngineDataStoreBenchmarkRecording
    {
        public string Id { get; init; }

        public long Processed { get; init; }

        public long Missed { get; init; }

        public TimeSpan Duration { get; init; }

        public IEnumerable<StatusReport> Snapshots { get; init; }

        public IEnumerable<EventDescriptor> Events { get; init; }

        public int Errors => this.Events?.Count(desc => (desc.Exception != null) || (desc.Severity == EventSeverity.Error)) ?? 0;
    }
}
