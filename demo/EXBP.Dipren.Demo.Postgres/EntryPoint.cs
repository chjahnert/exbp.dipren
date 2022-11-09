
using System.CommandLine;

namespace EXBP.Dipren.Demo.Postgres
{
    internal class EntryPoint
    {
        private const int DEFAULT_DATASET_SIZE = 100_000;


        internal static async Task<int> Main(string[] args)
        {
            RootCommand commandRoot = new RootCommand(EntryPointResources.DescriptionCommandRoot);

            Command commandDeploy = new Command("deploy", EntryPointResources.DescriptionCommandDeploy);

            Option<string> optionDatabase = new Option<string>( "--database", EntryPointResources.DescriptionOptionDatabase)
            {
                IsRequired = true
            };

            Option<int> optionDatasetSize = new Option<int>("--size", () => DEFAULT_DATASET_SIZE, EntryPointResources.DescriptionOptionDatasetSize);

            commandDeploy.AddOption(optionDatabase);
            commandDeploy.AddOption(optionDatasetSize);

            commandDeploy.SetHandler(EntryPoint.DeployHandlerAsync, optionDatabase, optionDatasetSize);

            commandRoot.Add(commandDeploy);

            commandRoot.SetHandler(EntryPoint.RootHandlerAsync);

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
    }
}
