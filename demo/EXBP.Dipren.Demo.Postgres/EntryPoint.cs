
using System.CommandLine;

using EXBP.Dipren.Demo.Postgres.Commands;


namespace EXBP.Dipren.Demo.Postgres
{
    internal class EntryPoint
    {
        private const int DEFAULT_DATASET_SIZE = 100_000;
        private const int DEFAULT_BATCH_PROCESSING_TIMEOUT_MS = 1000;
        private const int DEFAULT_BATCH_SIZE = 64;
        private const int DEFAULT_CLOCK_DRIFT_MS = 1000;
        private const int DEFAULT_PROCESSING_THREADS = 7;


        internal static async Task<int> Main(string[] args)
        {
            RootCommand commandRoot = new RootCommand(EntryPointResources.DescriptionCommandRoot);

            commandRoot.SetHandler(EntryPoint.HandleAsync);

            Option<string> optionDatabase = new Option<string>( "--database", EntryPointResources.DescriptionOptionDatabase)
            {
                IsRequired = true
            };

            Option<string> optionName = new Option<string>("--name", EntryPointResources.DescriptionOptionName)
            {
                IsRequired = true
            };


            Command commandDeploy = new Command("deploy", EntryPointResources.DescriptionCommandDeploy);
            Option<int> optionDeployDatasetSize = new Option<int>("--size", () => DEFAULT_DATASET_SIZE, EntryPointResources.DescriptionOptionDeployDatasetSize);

            commandDeploy.Add(optionDatabase);
            commandDeploy.Add(optionDeployDatasetSize);
            commandDeploy.SetHandler(Deploy.HandleAsync, optionDatabase, optionDeployDatasetSize);
            commandRoot.Add(commandDeploy);

            Command commandRemove = new Command("remove", EntryPointResources.DescriptionCommandRemove);

            commandRemove.Add(optionDatabase);
            commandRemove.SetHandler(Remove.HandleAsync, optionDatabase);
            commandRoot.Add(commandRemove);

            Command commandSchedule = new Command("schedule", EntryPointResources.DescriptionCommandSchedule);

            commandSchedule.Add(optionDatabase);
            commandSchedule.Add(optionName);
            commandSchedule.SetHandler(Schedule.HandleAsync, optionDatabase, optionName);
            commandRoot.Add(commandSchedule);

            Command commandProcess = new Command("process", EntryPointResources.DescriptionCommandProcess);
            Option<int> optionProcessThreads = new Option<int>("--threads", () => DEFAULT_PROCESSING_THREADS, EntryPointResources.DescriptionOptionProcessThreads);
            Option<TimeSpan> optionProcessBatchTimeout = new Option<TimeSpan>("--batch-timeout", () => TimeSpan.FromMilliseconds(DEFAULT_BATCH_PROCESSING_TIMEOUT_MS), EntryPointResources.DescriptionOptionProcessBatchTimeout);
            Option<int> optionProcessBatchSize = new Option<int>("--batch-size", () => DEFAULT_BATCH_SIZE, EntryPointResources.DescriptionOptionProcessThreads);
            Option<TimeSpan> optionProcessClockDrift = new Option<TimeSpan>("--clock-drift", () => TimeSpan.FromMilliseconds(DEFAULT_CLOCK_DRIFT_MS), EntryPointResources.DescriptionOptionProcessClockDrift);

            commandProcess.Add(optionDatabase);
            commandProcess.Add(optionName);
            commandProcess.Add(optionProcessThreads);
            commandProcess.Add(optionProcessBatchSize);
            commandProcess.Add(optionProcessBatchTimeout);
            commandProcess.Add(optionProcessClockDrift);
            commandProcess.SetHandler(Process.HandleAsync, optionDatabase, optionProcessThreads, optionName, optionProcessBatchSize, optionProcessBatchTimeout, optionProcessClockDrift);
            commandRoot.Add(commandProcess);

            await commandRoot.InvokeAsync(args);

            return 0;
        }

        private static Task<int> HandleAsync()
        {
            Console.WriteLine(EntryPointResources.DescriptionCommandRoot);

            return Task.FromResult(0);
        }
    }
}
