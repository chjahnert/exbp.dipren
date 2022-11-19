
using Npgsql;

namespace EXBP.Dipren.Demo.Postgres.Commands
{
    internal static class Remove
    {
        internal static async Task<int> HandleAsync(string connectionString)
        {
            int result = 0;

            NpgsqlDataSourceBuilder builder = new NpgsqlDataSourceBuilder(connectionString);
            NpgsqlDataSource dataSource = builder.Build();

            try
            {
                Console.Write(RemoveResources.MessageRemovingDemoSchema);
                await Database.ExecuteNonQueryAsync(dataSource, RemoveQueries.DropDemoDatabaseSchema);
                Console.WriteLine(RemoveResources.MessageDone);

                Console.Write(RemoveResources.MessageRemovingDiprenSchema);
                await Database.ExecuteNonQueryAsync(dataSource, RemoveQueries.DropDiprenDatabaseSchema);
                Console.WriteLine(RemoveResources.MessageDone);
            }
            catch (Exception ex)
            {
                Console.WriteLine(DeployResources.MessageFailed);
                Console.WriteLine();
                Console.WriteLine(ex);

                result = -1;
            }

            return result;
        }
    }
}
