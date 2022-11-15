
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

            Console.WriteLine("Timestamp           | State      | Started             | Completed           | Partitions | Untouched | In Progress | Completed | Keys Completed | Keys Remaining | Takeovers | Split Requests");
            Console.WriteLine("                    |            |                     |                     |            |           |             |           |                |                |           |");


            PostgresEngineDataStore store = new PostgresEngineDataStore(connectionString);
            Scheduler scheduler = new Scheduler(store, DebugEventLogger.Debug);

            Summary summary = null;

            do
            {
                try
                {
                    summary = await scheduler.GetJobStateAsync(name, CancellationToken.None);

                    string timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                    string started = summary.Started?.ToString("yyyy-MM-dd HH:mm:ss") ?? "n/a";
                    string completed = summary.Completed?.ToString("yyyy-MM-dd HH:mm:ss") ?? "n/a";

                    Console.WriteLine($"{timestamp} | {summary.State,-10} | {started,-19} | {completed,-19} | {summary.Partitions.Total,10} | {summary.Partitions.Untouched,9} | {summary.Partitions.InProgress,11} | {summary.Partitions.Completed,9} | {summary.Keys.Completed,14} | {summary.Keys.Remaining,14} | {summary.OwnershipChanges,9} | {summary.PendingSplitRequests,14}");
                }
                catch (UnknownIdentifierException)
                {
                }

                await Task.Delay(100);
            }
            while ((summary?.State != JobState.Completed) && (summary?.State != JobState.Failed));

            Console.ReadLine();

            return result;
        }
    }
}
