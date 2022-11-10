
namespace EXBP.Dipren.Demo.Postgres.Commands
{
    internal static class Process
    {
        internal static Task<int> HandleAsync(string connectionString, string name, int threads)
        {
            Console.WriteLine("PROCESS");
            Console.WriteLine();
            Console.WriteLine($"database: {connectionString}");
            Console.WriteLine($"name:     {name}");
            Console.WriteLine($"threads:  {threads}");

            return Task.FromResult(0);
        }
    }
}
