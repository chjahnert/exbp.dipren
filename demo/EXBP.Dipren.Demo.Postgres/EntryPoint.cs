
using System.CommandLine;

namespace EXBP.Dipren.Demo.Postgres
{
    internal class EntryPoint
    {
        internal static async Task<int> Main(string[] args)
        {
            RootCommand root = new RootCommand(EntryPointResources.ApplicationDescription);

            root.SetHandler(() =>
            {
                Console.WriteLine("Hello world!");
            });

            await root.InvokeAsync(args);

            return 0;
        }
    }
}
