
using System.CommandLine;

namespace EXBP.Dipren.Demo.Postgres
{
    internal class EntryPoint
    {
        private const int DEFAULT_DATASET_SIZE = 100_000;


        internal static async Task<int> Main(string[] args)
        {
            RootCommand commandRoot = new RootCommand(EntryPointResources.DescriptionCommandRoot);

            commandRoot.SetHandler(EntryPoint.RootHandlerAsync);

            Option<string> optionDatabase = new Option<string>( "--database", EntryPointResources.DescriptionOptionDatabase)
            {
                IsRequired = true
            };

            Command commandDeploy = new Command("deploy", EntryPointResources.DescriptionCommandDeploy);
            Option<int> optionDeployDatasetSize = new Option<int>("--size", () => DEFAULT_DATASET_SIZE, EntryPointResources.DescriptionOptionDatasetSize);

            commandDeploy.AddOption(optionDatabase);
            commandDeploy.AddOption(optionDeployDatasetSize);
            commandDeploy.SetHandler(EntryPoint.DeployHandlerAsync, optionDatabase, optionDeployDatasetSize);
            commandRoot.Add(commandDeploy);

            Command commandRemove = new Command("remove", EntryPointResources.DescriptionCommandRemove);

            commandRemove.AddOption(optionDatabase);
            commandRemove.SetHandler(EntryPoint.RemoveHandlerAsync, optionDatabase);
            commandRoot.Add(commandRemove);

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
            Console.WriteLine($"db: {connectionString}");
            Console.WriteLine($"ds: {datasetSize}");

            return Task.FromResult(0);
        }

        private static Task<int> RemoveHandlerAsync(string connectionString)
        {
            Console.WriteLine($"db: {connectionString}");

            return Task.FromResult(0);
        }
    }
}
