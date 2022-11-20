
using System.Diagnostics;

using EXBP.Dipren.Data;
using EXBP.Dipren.Data.Postgres;
using EXBP.Dipren.Demo.Postgres.Processing;
using EXBP.Dipren.Demo.Postgres.Processing.Models;
using EXBP.Dipren.Telemetry;


namespace EXBP.Dipren.Demo.Postgres.Commands
{
    internal static class Process
    {
        internal static async Task<int> HandleAsync(string connectionString, int threads, string name, bool reverse)
        {
            int result = 0;

            try
            {
                Console.WriteLine(ProcessResources.MessageStartingProcessing, name, threads);
                Console.WriteLine();

                Stopwatch stopwatch = Stopwatch.StartNew();

                Task[] tasks = new Task[threads];

                for (int i = 0; i < tasks.Length; i++)
                {
                    tasks[i] = Task.Run(async () => await Process.RunAsync(connectionString, name, reverse));
                }

                await Task.WhenAll(tasks);

                Console.WriteLine();
                Console.WriteLine(ProcessResources.MessageCompleted, stopwatch.Elapsed);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ProcessResources.MessageFailed);
                Console.WriteLine();
                Console.WriteLine(ex);

                result = -1;
            }

            return result;
        }

        internal static async Task RunAsync(string connectionString, string name, bool reverse)
        {
            Configuration configuration = new Configuration
            {
                PollingInterval = TimeSpan.FromMilliseconds(100)
            };

            IDataSource<Guid, Cuboid> source = reverse ? new CuboidDescendingDataSource(connectionString) : new CuboidAscendingDataSource(connectionString);
            CubiodBatchProcessor processor = new CubiodBatchProcessor(connectionString);
            Job<Guid, Cuboid> job = new Job<Guid, Cuboid>(name, source, GuidKeyArithmetics.LexicographicalMemberwise, GuidKeySerializer.Default, processor);

            await using (PostgresResilientEngineDataStore store = new PostgresResilientEngineDataStore(connectionString))
            {
                Engine engine = new Engine(store, ConsoleEventLogger.Information, configuration: configuration);

                await engine.RunAsync(job, false);
            }
        }
    }
}
