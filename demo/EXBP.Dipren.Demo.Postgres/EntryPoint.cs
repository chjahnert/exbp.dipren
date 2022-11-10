
using System.CommandLine;

using EXBP.Dipren.Demo.Postgres.Commands;


namespace EXBP.Dipren.Demo.Postgres
{
    internal class EntryPoint
    {
        private const int DEFAULT_DATASET_SIZE = 100_000;
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

            Command commandRun = new Command("run", EntryPointResources.DescriptionCommandRun);
            Option<int> optionRunThreads = new Option<int>("--threads", () => DEFAULT_PROCESSING_THREADS, EntryPointResources.DescriptionOptionRunThreads);

            commandRun.Add(optionDatabase);
            commandRun.Add(optionName);
            commandRun.Add(optionRunThreads);
            commandRun.SetHandler(Run.HandleAsync, optionDatabase, optionName, optionRunThreads);
            commandRoot.Add(commandRun);

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
