
using System.CommandLine;

using EXBP.Dipren.Demo.Postgres.Commands;

using Monitor = EXBP.Dipren.Demo.Postgres.Commands.Monitor;


namespace EXBP.Dipren.Demo.Postgres
{
    internal class EntryPoint
    {
        private const int DEFAULT_DATASET_SIZE = 100_000;
        private const int DEFAULT_BATCH_PROCESSING_TIMEOUT_MS = 2000;
        private const int DEFAULT_BATCH_SIZE = 128;
        private const int DEFAULT_CLOCK_DRIFT_MS = 1000;
        private const int DEFAULT_PROCESSING_THREADS = 3;


        internal static async Task<int> Main(string[] args)
        {
            RootCommand commandRoot = new RootCommand(EntryPointResources.DescriptionCommandRoot);

            commandRoot.SetHandler(EntryPoint.HandleAsync);

            Option<string> optionDatabase = new Option<string>( "--database", EntryPointResources.DescriptionOptionDatabase)
            {
                IsRequired = true
            };

            Argument<string> argumentJob = new Argument<string>("job-name", EntryPointResources.DescriptionArgumentJobName);

            Option<bool> optionsReverse = new Option<bool>("--reverse", () => false, EntryPointResources.DescriptionOptionReverse);

            Command commandDeploy = new Command("deploy", EntryPointResources.DescriptionCommandDeploy);
            Option<int> optionDeployDatasetSize = new Option<int>("--size", () => DEFAULT_DATASET_SIZE, EntryPointResources.DescriptionOptionDeployDatasetSize);

            commandDeploy.Add(optionDatabase);
            commandDeploy.Add(optionDeployDatasetSize);
            commandDeploy.SetHandler(Deploy.HandleAsync, optionDatabase, optionDeployDatasetSize);
            commandRoot.Add(commandDeploy);

            Command commandSchedule = new Command("schedule", EntryPointResources.DescriptionCommandSchedule);

            commandSchedule.Add(optionDatabase);
            commandSchedule.Add(argumentJob);
            commandSchedule.Add(optionsReverse);
            commandSchedule.SetHandler(Schedule.HandleAsync, optionDatabase, argumentJob, optionsReverse);
            commandRoot.Add(commandSchedule);

            Command commandProcess = new Command("process", EntryPointResources.DescriptionCommandProcess);
            Option<int> optionProcessThreads = new Option<int>("--threads", () => DEFAULT_PROCESSING_THREADS, EntryPointResources.DescriptionOptionProcessThreads);
            Option<TimeSpan> optionProcessBatchTimeout = new Option<TimeSpan>("--batch-timeout", () => TimeSpan.FromMilliseconds(DEFAULT_BATCH_PROCESSING_TIMEOUT_MS), EntryPointResources.DescriptionOptionProcessBatchTimeout);
            Option<int> optionProcessBatchSize = new Option<int>("--batch-size", () => DEFAULT_BATCH_SIZE, EntryPointResources.DescriptionOptionProcessThreads);
            Option<TimeSpan> optionProcessClockDrift = new Option<TimeSpan>("--clock-drift", () => TimeSpan.FromMilliseconds(DEFAULT_CLOCK_DRIFT_MS), EntryPointResources.DescriptionOptionProcessClockDrift);

            commandProcess.Add(optionDatabase);
            commandProcess.Add(argumentJob);
            commandProcess.Add(optionsReverse);
            commandProcess.Add(optionProcessThreads);
            commandProcess.Add(optionProcessBatchSize);
            commandProcess.Add(optionProcessBatchTimeout);
            commandProcess.Add(optionProcessClockDrift);
            commandProcess.SetHandler(Process.HandleAsync, optionDatabase, optionProcessThreads, argumentJob, optionsReverse, optionProcessBatchSize, optionProcessBatchTimeout, optionProcessClockDrift);
            commandRoot.Add(commandProcess);

            Command commandRemove = new Command("remove", EntryPointResources.DescriptionCommandRemove);

            commandRemove.Add(optionDatabase);
            commandRemove.SetHandler(Remove.HandleAsync, optionDatabase);
            commandRoot.Add(commandRemove);

            Command commandMonitor = new Command("monitor", EntryPointResources.DescriptionCommandMonitor);

            commandMonitor.Add(optionDatabase);
            commandMonitor.Add(argumentJob);
            commandMonitor.SetHandler(Monitor.HandleAsync, optionDatabase, argumentJob);
            commandRoot.Add(commandMonitor);

            await commandRoot.InvokeAsync(args);

            return 0;
        }

        private static Task<int> HandleAsync()
        {
            Console.WriteLine(EntryPointResources.MessageIntroduction);

            return Task.FromResult(0);
        }
    }
}
