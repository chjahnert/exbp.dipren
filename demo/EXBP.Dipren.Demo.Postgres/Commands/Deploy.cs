
namespace EXBP.Dipren.Demo.Postgres.Commands
{
    internal static class Deploy
    {
        internal static async Task<int> HandleAsync(string database, int size)
        {
            int result = 0;

            try
            {
                Console.Write(DeployResources.MessageCreatingDemoSchema);
                await Database.ExecuteNonQueryAsync(database, DeployQueries.DropDemoDatabaseSchema);
                await Database.ExecuteNonQueryAsync(database, DeployQueries.CreateDemoDatabaseSchema);
                Console.WriteLine(DeployResources.MessageDone);

                Console.Write(DeployResources.MessageCreatingDiprenSchema);
                await Database.ExecuteNonQueryAsync(database, DeployQueries.DropDiprenDatabaseSchema);
                await Database.ExecuteNonQueryAsync(database, DeployQueries.CreateDiprenDatabaseSchema);
                Console.WriteLine(DeployResources.MessageDone);

                Console.Write(DeployResources.MessageGeneratingDimensions, size);
                await Database.ExecuteNonQueryAsync(database, DeployQueries.GenerateDimensions, ("@limit", size));
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
