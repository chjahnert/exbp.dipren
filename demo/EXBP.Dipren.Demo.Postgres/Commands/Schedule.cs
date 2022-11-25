﻿
using EXBP.Dipren.Data;
using EXBP.Dipren.Data.Postgres;
using EXBP.Dipren.Demo.Postgres.Processing;
using EXBP.Dipren.Demo.Postgres.Processing.Models;
using EXBP.Dipren.Demo.Postgres.Processing.Resilience;
using EXBP.Dipren.Telemetry;


namespace EXBP.Dipren.Demo.Postgres.Commands
{
    internal static class Schedule
    {
        internal static async Task<int> HandleAsync(string connectionString, string name, int batchSize, TimeSpan batchTimeout, TimeSpan clockDrift, bool reverse)
        {
            int result = 0;

            try
            {
                Console.Write(ScheduleResources.MessageSchedulingJob, name);

                await using (PostgresEngineDataStore store = new PostgresEngineDataStore(connectionString))
                {
                    Scheduler scheduler = new Scheduler(store, DebugEventLogger.Debug);

                    IDataSource<Guid, Cuboid> source = new ResilientDataSource<Guid, Cuboid>(reverse ? new CuboidDescendingDataSource(connectionString) : new CuboidAscendingDataSource(connectionString));
                    IBatchProcessor<Cuboid> processor = new ResilientBatchProcessor<Cuboid>(new CubiodBatchProcessor(connectionString));
                    Job<Guid, Cuboid> job = new Job<Guid, Cuboid>(name, source, GuidKeyArithmetics.LexicographicalMemberwise, GuidKeySerializer.Default, processor);
                    Settings settings = new Settings(batchSize, batchTimeout, clockDrift);

                    await scheduler.ScheduleAsync(job, settings);
                }

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
