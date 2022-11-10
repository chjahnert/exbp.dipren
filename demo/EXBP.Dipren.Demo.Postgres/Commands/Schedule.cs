
namespace EXBP.Dipren.Demo.Postgres.Commands
{
    internal static class Schedule
    {
        internal static Task<int> HandleAsync(string connectionString, string name)
        {
            Console.WriteLine("SCHEDULE");
            Console.WriteLine();
            Console.WriteLine($"database: {connectionString}");
            Console.WriteLine($"name:     {name}");

            return Task.FromResult(0);
        }
    }
}
