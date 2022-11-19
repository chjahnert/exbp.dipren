
using Npgsql;

namespace EXBP.Dipren.Demo.Postgres.Commands
{
    internal static class Deploy
    {
        internal static async Task<int> HandleAsync(string connectionString, int size)
        {
            int result = 0;

            NpgsqlDataSourceBuilder builder = new NpgsqlDataSourceBuilder(connectionString);
            NpgsqlDataSource dataSource = builder.Build();

            try
            {
                Console.Write(DeployResources.MessageCreatingDemoSchema);
                await Database.ExecuteNonQueryAsync(dataSource, DeployQueries.DropDemoDatabaseSchema);
                await Database.ExecuteNonQueryAsync(dataSource, DeployQueries.CreateDemoDatabaseSchema);
                Console.WriteLine(DeployResources.MessageDone);

                Console.Write(DeployResources.MessageCreatingDiprenSchema);
                await Database.ExecuteNonQueryAsync(dataSource, DeployQueries.DropDiprenDatabaseSchema);
                await Database.ExecuteNonQueryAsync(dataSource, DeployQueries.CreateDiprenDatabaseSchema);
                Console.WriteLine(DeployResources.MessageDone);

                Console.Write(DeployResources.MessageGeneratingDimensions, size);
                await Database.ExecuteNonQueryAsync(dataSource, DeployQueries.GenerateCuboids, ("@limit", size));
                Console.WriteLine(DeployResources.MessageDone);
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
