
using EXBP.Dipren.Data;
using EXBP.Dipren.Data.Postgres;
using EXBP.Dipren.Demo.Postgres.Processing;
using EXBP.Dipren.Demo.Postgres.Processing.Models;
using EXBP.Dipren.Telemetry;


namespace EXBP.Dipren.Demo.Postgres.Commands
{
    internal static class Schedule
    {
        internal static async Task<int> HandleAsync(string connectionString, string name, bool reverse)
        {
            int result = 0;

            try
            {
                Console.Write(ScheduleResources.MessageSchedulingJob, name);

                PostgresEngineDataStore store = new PostgresEngineDataStore(connectionString);
                Scheduler scheduler = new Scheduler(store, DebugEventLogger.Debug);

                TimeSpan timeout = TimeSpan.FromSeconds(1);
                IDataSource<Guid, Cuboid> source = reverse ? new CuboidDescendingDataSource(connectionString) : new CuboidAscendingDataSource(connectionString);
                CubiodBatchProcessor processor = new CubiodBatchProcessor(connectionString);
                Job<Guid, Cuboid> job = new Job<Guid, Cuboid>(name, source, GuidKeyArithmetics.LexicographicalMemberwise, GuidKeySerializer.Default, processor, timeout, 1);

                await scheduler.ScheduleAsync(job);

                Console.WriteLine(RemoveResources.MessageDone);
            }
            catch (Exception ex)
            {
                Console.WriteLine(DeployResources.MessageFailed);
                Console.WriteLine();
                Console.WriteLine(ex);

                result = -1;
            }

            return result;
        }
    }
}
