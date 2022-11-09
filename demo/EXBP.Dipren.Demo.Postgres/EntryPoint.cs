
using System.CommandLine;

namespace EXBP.Dipren.Demo.Postgres
{
    internal class EntryPoint
    {
        private const int DEFAULT_DATASET_SIZE = 100_000;
        private const int DEFAULT_PROCESSING_THREADS = 7;


        internal static async Task<int> Main(string[] args)
        {
            RootCommand commandRoot = new RootCommand(EntryPointResources.DescriptionCommandRoot);

            commandRoot.SetHandler(EntryPoint.RootHandlerAsync);

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
            commandDeploy.SetHandler(EntryPoint.DeployHandlerAsync, optionDatabase, optionDeployDatasetSize);
            commandRoot.Add(commandDeploy);

            Command commandRemove = new Command("remove", EntryPointResources.DescriptionCommandRemove);

            commandRemove.Add(optionDatabase);
            commandRemove.SetHandler(EntryPoint.RemoveHandlerAsync, optionDatabase);
            commandRoot.Add(commandRemove);

            Command commandSchedule = new Command("schedule", EntryPointResources.DescriptionCommandSchedule);

            commandSchedule.Add(optionDatabase);
            commandSchedule.Add(optionName);
            commandSchedule.SetHandler(EntryPoint.ScheduleHandlerAsync, optionDatabase, optionName);
            commandRoot.Add(commandSchedule);

            Command commandRun = new Command("run", EntryPointResources.DescriptionCommandRun);
            Option<int> optionRunThreads = new Option<int>("--threads", () => DEFAULT_PROCESSING_THREADS, EntryPointResources.DescriptionOptionRunThreads);

            commandRun.Add(optionDatabase);
            commandRun.Add(optionName);
            commandRun.Add(optionRunThreads);
            commandRun.SetHandler(EntryPoint.RunHandlerAsync, optionDatabase, optionName, optionRunThreads);
            commandRoot.Add(commandRun);

            await commandRoot.InvokeAsync(args);

            return 0;
        }

        private static Task<int> RootHandlerAsync()
        {
            Console.WriteLine(EntryPointResources.DescriptionCommandRoot);

            return Task.FromResult(0);
        }

        private static Task<int> DeployHandlerAsync(string connectionString, int datasetSize)
        {
            Console.WriteLine("DEPLOY");
            Console.WriteLine();
            Console.WriteLine($"database: {connectionString}");
            Console.WriteLine($"size:     {datasetSize}");

            return Task.FromResult(0);
        }

        private static Task<int> RemoveHandlerAsync(string connectionString)
        {
            Console.WriteLine("REMOVE");
            Console.WriteLine();
            Console.WriteLine($"database: {connectionString}");

            return Task.FromResult(0);
        }

        private static Task<int> ScheduleHandlerAsync(string connectionString, string name)
        {
            Console.WriteLine("SCHEDULE");
            Console.WriteLine();
            Console.WriteLine($"database: {connectionString}");
            Console.WriteLine($"name:     {name}");

            return Task.FromResult(0);
        }

        private static Task<int> RunHandlerAsync(string connectionString, string name, int threads)
        {
            Console.WriteLine("RUN");
            Console.WriteLine();
            Console.WriteLine($"database: {connectionString}");
            Console.WriteLine($"name:     {name}");
            Console.WriteLine($"threads:  {threads}");

            return Task.FromResult(0);
        }
    }
}
