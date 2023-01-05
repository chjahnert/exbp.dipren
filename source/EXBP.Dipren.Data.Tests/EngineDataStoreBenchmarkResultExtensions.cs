
using System.Diagnostics;
using System.Globalization;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Data.Tests
{
    public static class EngineDataStoreBenchmarkResultExtensions
    {
        private const string CSV_DELIMITER = "\t";

        public static async Task SaveSnapshotsAsync(this EngineDataStoreBenchmarkResult result, string fileName)
        {
            await using StreamWriter writer = new StreamWriter(fileName, false);

            await result.SaveSnapshotsAsync(writer, true);
        }


        public static async Task SaveSnapshotsAsync(this EngineDataStoreBenchmarkResult result, TextWriter writer, bool headers)
        {
            Assert.ArgumentIsNotNull(result, nameof(result));
            Assert.ArgumentIsNotNull(writer, nameof(writer));

            if (headers == true)
            {
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "Timestamp", true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "State", true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "Started", true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "Completed", true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "Partitions", true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "Partitions Pending", true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "Partitions In Progress", true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "Partitions Completed", true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "Keys Completed", true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "Keys Remaining", true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "Progress", true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "Takeovers", true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "Split Requests", true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "Throughput", true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, "Remaining Time", false);

                await writer.WriteLineAsync();
            }

            foreach (StatusReport snapshot in result.Snapshots.OrderBy(s => s.Timestamp))
            {
                TimeSpan? eta = ((snapshot.Progress.Remaining != null) && (snapshot.CurrentThroughput > 0.0)) ? TimeSpan.FromSeconds(snapshot.Progress.Remaining.Value / snapshot.CurrentThroughput) : null;

                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, snapshot.Timestamp, true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, snapshot.State, true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, snapshot.Started, true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, snapshot.Completed, true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, snapshot.Partitions.Total, true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, snapshot.Partitions.Untouched, true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, snapshot.Partitions.InProgress, true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, snapshot.Partitions.Completed, true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, snapshot.Progress.Completed, true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, snapshot.Progress.Remaining, true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, snapshot.Progress.Ratio, true, true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, snapshot.OwnershipChanges, true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, snapshot.PendingSplitRequests, true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, snapshot.CurrentThroughput, false, true);
                await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, eta, false);

                await writer.WriteLineAsync();
            }
        }

        private static async Task WriteFactAsync(TextWriter writer, string fact, bool delimitter)
        {
            Debug.Assert(writer != null);

            await writer.WriteAsync(fact);

            if (delimitter == true)
            {
                await writer.WriteAsync(CSV_DELIMITER);
            }
        }

        private static async Task WriteFactAsync(TextWriter writer, DateTime? value, bool delimitter)
        {
            string formatted = value?.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture) ?? string.Empty;

            await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, formatted, delimitter);
        }

        private static async Task WriteFactAsync(TextWriter writer, TimeSpan? value, bool delimitter)
        {
            string formatted = value?.ToString("c", CultureInfo.InvariantCulture) ?? string.Empty;

            await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, formatted, delimitter);
        }

        private static async Task WriteFactAsync(TextWriter writer, JobState value, bool delimitter)
        {
            string formatted = value.ToString();

            await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, formatted, delimitter);
        }

        private static async Task WriteFactAsync(TextWriter writer, long? value, bool delimitter)
        {
            string formatted = value?.ToString("D", CultureInfo.InvariantCulture) ?? string.Empty;

            await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, formatted, delimitter);
        }

        private static async Task WriteFactAsync(TextWriter writer, double? value, bool perscentage, bool delimitter)
        {
            string formatted = value?.ToString((perscentage ? "P" : "G"), CultureInfo.InvariantCulture) ?? string.Empty;

            await EngineDataStoreBenchmarkResultExtensions.WriteFactAsync(writer, formatted, delimitter);
        }
    }
}
