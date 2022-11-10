
namespace EXBP.Dipren.Demo.Postgres.Commands
{
    internal static class Deploy
    {
        internal static Task<int> HandleAsync(string connectionString, int datasetSize)
        {
            Console.WriteLine("DEPLOY");
            Console.WriteLine();
            Console.WriteLine($"database: {connectionString}");
            Console.WriteLine($"size:     {datasetSize}");

            return Task.FromResult(0);
        }
    }
}
