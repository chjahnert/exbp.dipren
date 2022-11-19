
using System.Diagnostics;

using Npgsql;


namespace EXBP.Dipren.Demo.Postgres
{
    internal static class Database
    {
        internal static async Task<int> ExecuteNonQueryAsync(NpgsqlDataSource dataSource, string text, params (string, object)[] parameters)
        {
            Debug.Assert(dataSource != null);
            Debug.Assert(text != null);

            await using NpgsqlCommand command = dataSource.CreateCommand(text);

            for (int i = 0; i < parameters.Length; i++)
            {
                command.Parameters.AddWithValue(parameters[i].Item1, parameters[i].Item2);
            }

            int result = (int) await command.ExecuteNonQueryAsync();

            return result;
        }
    }
}
