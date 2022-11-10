
namespace EXBP.Dipren.Demo.Postgres.Commands
{
    internal static class Remove
    {
        internal static Task<int> HandleAsync(string connectionString)
        {
            Console.WriteLine("REMOVE");
            Console.WriteLine();
            Console.WriteLine($"database: {connectionString}");

            return Task.FromResult(0);
        }
    }
}
