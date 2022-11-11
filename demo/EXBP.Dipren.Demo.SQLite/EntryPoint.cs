
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
        private const string TARGET_CONNECTION_STRING = "Data Source = target.sqlite.db; DateTimeKind = UTC;";
        private const string ENGINE_CONNECTION_STRING = "Data Source = dipren.sqlite.db; DateTimeKind = UTC;";

        private const int SOURCE_ITEM_COUNT = 70000;
        private const int BATCH_PROCESSING_TIMEOUT = 1000;
        private const int PROCESSING_THREADS = 7;
        private const int POLLING_INTERVAL = 25;
        private const int BATCH_SIZE = 256;

        private static MeasurementBatchProcessor Processor { get; } = new MeasurementBatchProcessor(TARGET_CONNECTION_STRING, (BATCH_SIZE * PROCESSING_THREADS * 2));


        internal static async Task Main()
        {
            File.Delete("source.sqlite.db");
            File.Delete("target.sqlite.db");
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

                Job<int, Measurement> job = EntryPoint.CreateJob(connectionSource);

                SQLiteEngineDataStore store = new SQLiteEngineDataStore(ENGINE_CONNECTION_STRING);
                Scheduler scheduler = new Scheduler(store);

                await scheduler.ScheduleAsync(job);

                Console.WriteLine("done.");

                Console.WriteLine("Starting up the processing threads.");
                Console.WriteLine();

                stopwatch.Start();

                Task[] tasks = new Task[PROCESSING_THREADS];

                for (int i = 0; i < tasks.Length; i++)
                {
                    tasks[i] = Task.Run(async () => await EntryPoint.RunAsync(store, job));
                }

                Task.WaitAll(tasks);

                EntryPoint.Processor.Flush();

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

        internal static Job<int, Measurement> CreateJob(SQLiteConnection connectionSource)
        {
            MeasurementsDataSource source = new MeasurementsDataSource(connectionSource);
            TimeSpan timeout = TimeSpan.FromMilliseconds(BATCH_PROCESSING_TIMEOUT);

            Job<int, Measurement> result = new Job<int, Measurement>("measurements", source, Int32KeyArithmetics.Default, Int32KeySerializer.Default, EntryPoint.Processor, timeout, BATCH_SIZE);

            return result;
        }

        internal static async Task RunAsync(IEngineDataStore store, Job<int, Measurement> job)
        {
            TimeSpan pollinInterval = TimeSpan.FromMilliseconds(POLLING_INTERVAL);
            Configuration configuration = new Configuration(TimeSpan.Zero, pollinInterval);
            Engine engine = new Engine(store, ConsoleEventLogger.Information, UtcDateTimeProvider.Default);

            await engine.RunAsync(job, false);
        }
    }
}