
using System.Data;
using System.Diagnostics;

using Npgsql;


namespace EXBP.Dipren.Demo.Postgres
{
    internal static class Database
    {
        internal static async Task<int> ExecuteNonQueryAsync(string connectionString, string text, params (string, object)[] parameters)
        {
            Debug.Assert(connectionString != null);
            Debug.Assert(text != null);

            int result = 0;

            using (NpgsqlConnection connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync();

                using NpgsqlCommand command = new NpgsqlCommand
                {
                    CommandText = text,
                    CommandType = CommandType.Text,
                    Connection = connection
                };

                for (int i = 0; i < parameters.Length; i++)
                {
                    command.Parameters.AddWithValue(parameters[i].Item1, parameters[i].Item2);
                }

                result = (int) await command.ExecuteNonQueryAsync();
            }

            return result;
        }
    }
}
