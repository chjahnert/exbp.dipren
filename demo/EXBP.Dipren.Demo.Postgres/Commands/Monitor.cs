
using EXBP.Dipren.Data;
using EXBP.Dipren.Data.Postgres;
using EXBP.Dipren.Telemetry;


namespace EXBP.Dipren.Demo.Postgres.Commands
{
    internal static class Monitor
    {
        internal static async Task<int> HandleAsync(string connectionString, string name)
        {
            int result = 0;

            Console.WriteLine("Timestamp           | State      | Started             | Completed           | Partitions | Untouched | In Progress | Completed | Keys Completed | Keys Remaining | Progress | Takeovers | Split Requests | Throughput | Remaining Time");
            Console.WriteLine("                    |            |                     |                     |            |           |             |           |                |                |          |           |                |            |");

            await using (PostgresResilientEngineDataStore store = new PostgresResilientEngineDataStore(connectionString))
            {
                Scheduler scheduler = new Scheduler(store, DebugEventLogger.Debug);

                StatusReport summary = null;

                do
                {
                    try
                    {
                        summary = await scheduler.GetStatusReportAsync(name, CancellationToken.None);

                        string timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                        string started = summary.Started?.ToString("yyyy-MM-dd HH:mm:ss") ?? "n/a";
                        string completed = summary.Completed?.ToString("yyyy-MM-dd HH:mm:ss") ?? "n/a";
                        double progress = (summary.Progress.Ratio != null) ? Math.Round(summary.Progress.Ratio.Value * 100, 1) : 0d;
                        TimeSpan? eta = ((summary.Progress.Remaining != null) && (summary.CurrentThroughput > 0.0)) ? TimeSpan.FromSeconds(summary.Progress.Remaining.Value / summary.CurrentThroughput) : null;

                        Console.WriteLine($"{timestamp} | {summary.State,-10} | {started,-19} | {completed,-19} | {summary.Partitions.Total,10} | {summary.Partitions.Untouched,9} | {summary.Partitions.InProgress,11} | {summary.Partitions.Completed,9} | {summary.Progress.Completed,14} | {summary.Progress.Remaining,14} | {progress,7:F1}% | {summary.OwnershipChanges,9} | {summary.PendingSplitRequests,14} | {summary.CurrentThroughput,10:F1} | {eta,14:hh\\:mm\\:ss\\.ff}");
                    }
                    catch (UnknownIdentifierException)
                    {
                    }

                    await Task.Delay(100);
                }
                while ((summary?.State != JobState.Completed) && (summary?.State != JobState.Failed));
            }

            Console.ReadLine();

            return result;
        }
    }
}
