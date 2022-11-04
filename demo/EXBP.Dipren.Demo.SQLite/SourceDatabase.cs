
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;


namespace EXBP.Dipren.Demo.SQLite
{
    internal static class SourceDatabase
    {
        private static readonly Random Random = new Random(8394738);


        internal static async Task<SQLiteConnection> CreateSourceDatabaseAsync(string connectionString)
        {
            SQLiteConnection result = new SQLiteConnection(connectionString);

            await result.OpenAsync();

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SourceDatabaseResources.QueryCreateTableMeasurements,
                CommandType = CommandType.Text,
                Connection = result
            };

            await command.ExecuteNonQueryAsync();

            return result;
        }


        internal static async Task PopulateSourceDatabaseAsync(SQLiteConnection connection, long count)
        {
            Debug.Assert(connection != null);
            Debug.Assert(count >= 0L);

            using SQLiteTransaction transaction = connection.BeginTransaction();

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = SourceDatabaseResources.QueryInsertMeasurement,
                CommandType = CommandType.Text,
                Connection = connection,
                Transaction = transaction
            };

            SQLiteParameter parameterId = new SQLiteParameter("$id");
            SQLiteParameter parameterWidth = new SQLiteParameter("width");
            SQLiteParameter parameterHeight = new SQLiteParameter("$height");
            SQLiteParameter parameterDepth = new SQLiteParameter("$depth");

            command.Parameters.Add(parameterId);
            command.Parameters.Add(parameterWidth);
            command.Parameters.Add(parameterHeight);
            command.Parameters.Add(parameterDepth);

            int id = 0;

            for (int i = 0; i < count; i++)
            {
                id += Random.Next(1, 7);

                parameterId.Value = i;
                parameterWidth.Value = Random.Next(1, 99);
                parameterHeight.Value = Random.Next(1, 99);
                parameterDepth.Value = Random.Next(1, 99);

                await command.ExecuteNonQueryAsync();
            }

            transaction.Commit();
        }
    }
}
