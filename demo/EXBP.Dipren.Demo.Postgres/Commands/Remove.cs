
namespace EXBP.Dipren.Demo.Postgres.Commands
{
    internal static class Remove
    {
        internal static async Task<int> HandleAsync(string connectionString)
        {
            int result = 0;

            try
            {
                Console.Write(RemoveResources.MessageRemovingDemoSchema);
                await Database.ExecuteNonQueryAsync(connectionString, RemoveQueries.DropDemoDatabaseSchema);
                Console.WriteLine(RemoveResources.MessageDone);

                Console.Write(RemoveResources.MessageRemovingDiprenSchema);
                await Database.ExecuteNonQueryAsync(connectionString, RemoveQueries.DropDiprenDatabaseSchema);
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
