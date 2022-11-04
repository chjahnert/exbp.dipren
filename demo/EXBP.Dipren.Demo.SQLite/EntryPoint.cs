
using System.Data.SQLite;
using System.Diagnostics;

using EXBP.Dipren.Data;
using EXBP.Dipren.Data.SQLite;
using EXBP.Dipren.Telemetry;


namespace EXBP.Dipren.Demo.SQLite
{
    internal static class EntryPoint
    {
        private const string SOURCE_CONNECTION_STRING = "Data Source = source.sqlite.db; DateTimeKind = UTC;";
        private const string ENGINE_CONNECTION_STRING = "Data Source = dipren.sqlite.db; DateTimeKind = UTC;";

        private const long SOURCE_ITEM_COUNT = 4096;
        private const int BATCH_PROCESSING_TIMEOUT = 1000;
        private const int BATCH_SIZE = 4;
        private const int THREADS = 8;


        internal static async Task Main()
        {
            File.Delete("source.sqlite.db");
            File.Delete("dipren.sqlite.db");

            Stopwatch stopwatch = new Stopwatch();

            try
            {
                Console.Write($"Creating source database ... ");

                SQLiteConnection connectionSource = await SourceDatabase.CreateSourceDatabaseAsync(SOURCE_CONNECTION_STRING);

                Console.WriteLine("done.");

                Console.Write($"Populating source database with {SOURCE_ITEM_COUNT} rows ... ");

                await SourceDatabase.PopulateSourceDatabaseAsync(connectionSource, SOURCE_ITEM_COUNT);

                Console.WriteLine("done.");

                Console.Write("Scheduling the processing job ... ");

                Job<int, Measurement> job = EntryPoint.ConstructJob(connectionSource);

                SQLiteEngineDataStore store = new SQLiteEngineDataStore(ENGINE_CONNECTION_STRING);
                Scheduler scheduler = new Scheduler(store);

                await scheduler.ScheduleAsync(job, CancellationToken.None);

                Console.WriteLine("done.");

                Console.WriteLine("Starting up the processing threads.");
                Console.WriteLine();

                stopwatch.Start();

                Task[] tasks = new Task[THREADS];

                for (int i = 0; i < tasks.Length; i++)
                {
                    tasks[i] = Task.Run(async () => await EntryPoint.RunAsync(store, job));
                }

                Task.WaitAll(tasks);

                stopwatch.Stop();

                Console.WriteLine();
                Console.WriteLine($"Terminating. Completed in {stopwatch.Elapsed}.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"failed. {ex.Message}");
                Console.WriteLine();
                Console.WriteLine(ex);
            }
        }

        internal static Job<int, Measurement> ConstructJob(SQLiteConnection connectionSource)
        {
            MeasurementsDataSource source = new MeasurementsDataSource(connectionSource);
            MeasurementBatchProcessor processor = new MeasurementBatchProcessor();
            TimeSpan timeout = TimeSpan.FromMilliseconds(BATCH_PROCESSING_TIMEOUT);

            Job<int, Measurement> result = new Job<int, Measurement>("measurements", source, Int32KeyArithmetics.Default, Int32KeySerializer.Default, processor, timeout, BATCH_SIZE);

            return result;
        }

        internal static async Task RunAsync(IEngineDataStore store, Job<int, Measurement> job)
        {
            Engine engine = new Engine(store, ConsoleEventLogger.Information, UtcDateTimeProvider.Default);

            await engine.RunAsync(job, false, CancellationToken.None);
        }
    }
}