
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
        internal static async Task<int> HandleAsync(string connectionString, int threads, string name, int batchSize, TimeSpan batchTimeout, TimeSpan clockDrift)
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
                    tasks[i] = Task.Run(async () => await Process.RunAsync(connectionString, name, batchSize, batchTimeout, clockDrift));
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

        internal static async Task RunAsync(string connectionString, string name, int batchSize, TimeSpan batchTimeout, TimeSpan clockDrift)
        {
            Configuration configuration = new Configuration
            {
                MaximumClockDrift = clockDrift,
                PollingInterval = TimeSpan.FromMilliseconds(100)
            };

            PostgresEngineDataStore store = new PostgresEngineDataStore(connectionString);
            Engine engine = new Engine(store, ConsoleEventLogger.Information, configuration: configuration);

            CuboidDataSource source = new CuboidDataSource(connectionString);
            CubiodBatchProcessor processor = new CubiodBatchProcessor(connectionString);
            Job<Guid, Cuboid> job = new Job<Guid, Cuboid>(name, source, GuidKeyArithmetics.LexicographicalOrder, GuidKeySerializer.Default, processor, batchTimeout, batchSize);

            await engine.RunAsync(job, false);
        }
    }
}
