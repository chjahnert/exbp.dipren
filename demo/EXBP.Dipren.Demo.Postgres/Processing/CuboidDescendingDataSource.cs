
using System.Data;
using System.Data.Common;
using System.Diagnostics;

using Npgsql;
using NpgsqlTypes;

using EXBP.Dipren.Demo.Postgres.Processing.Models;


namespace EXBP.Dipren.Demo.Postgres.Processing
{
    internal class CuboidDescendingDataSource : IDataSource<Guid, Cuboid>
    {
        private readonly string _connectionString;

        internal CuboidDescendingDataSource(string connectionString)
        {
            Debug.Assert(connectionString != null);

            this._connectionString = connectionString;
        }

        public async Task<long> EstimateRangeSizeAsync(Range<Guid> range, CancellationToken cancellation)
        {
            Debug.Assert(range != null);

            long result = 0L;

            using (NpgsqlConnection connection = new NpgsqlConnection(this._connectionString))
            {
                await connection.OpenAsync(cancellation);

                using NpgsqlCommand command = new NpgsqlCommand
                {
                    CommandText = "SELECT COUNT(1) AS \"count\" FROM \"demo\".\"cuboids\" WHERE (\"id\" <= @first) AND (((@inclusive = TRUE) AND (\"id\" >= @last)) OR ((@inclusive = FALSE) AND (\"id\" > @last)));",
                    CommandType = CommandType.Text,
                    Connection = connection
                };

                command.Parameters.AddWithValue("@first", NpgsqlDbType.Uuid, range.First);
                command.Parameters.AddWithValue("@last", NpgsqlDbType.Uuid, range.Last);
                command.Parameters.AddWithValue("@inclusive", NpgsqlDbType.Boolean, range.IsInclusive);

                result = (long) await command.ExecuteScalarAsync(cancellation);
            }

            return result;
        }

        public async Task<Range<Guid>> GetEntireRangeAsync(CancellationToken cancellation)
        {
            Range<Guid> result = null;

            using (NpgsqlConnection connection = new NpgsqlConnection(this._connectionString))
            {
                await connection.OpenAsync(cancellation);

                using NpgsqlCommand command = new NpgsqlCommand
                {
                    CommandText = "SELECT (SELECT \"id\" FROM \"demo\".\"cuboids\" ORDER BY \"id\" DESC LIMIT 1) AS \"first\", (SELECT \"id\" FROM \"demo\".\"cuboids\" ORDER BY \"id\" ASC LIMIT 1) AS \"last\";",
                    CommandType = CommandType.Text,
                    Connection = connection
                };

                using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
                {
                    reader.Read();

                    Guid first = await reader.GetFieldValueAsync<Guid>("first");
                    Guid last = await reader.GetFieldValueAsync<Guid>("last");

                    result = new Range<Guid>(first, last, true);
                }
            }

            return result;
        }

        public async Task<IEnumerable<KeyValuePair<Guid, Cuboid>>> GetNextBatchAsync(Range<Guid> range, int skip, int take, CancellationToken cancellation)
        {
            List<KeyValuePair<Guid, Cuboid>> result = null;

            using (NpgsqlConnection connection = new NpgsqlConnection(this._connectionString))
            {
                await connection.OpenAsync(cancellation);

                using NpgsqlCommand command = new NpgsqlCommand
                {
                    CommandText = "SELECT \"id\", \"width\", \"height\", \"depth\" FROM \"demo\".\"cuboids\" WHERE (\"id\" <= @first) AND (((@inclusive = TRUE) AND (\"id\" >= @last)) OR ((@inclusive = FALSE) AND (\"id\" > @last))) ORDER BY \"id\" DESC OFFSET @skip LIMIT @take;",
                    CommandType = CommandType.Text,
                    Connection = connection
                };

                command.Parameters.AddWithValue("@first", NpgsqlDbType.Uuid, range.First);
                command.Parameters.AddWithValue("@last", NpgsqlDbType.Uuid, range.Last);
                command.Parameters.AddWithValue("@inclusive", NpgsqlDbType.Boolean, range.IsInclusive);
                command.Parameters.AddWithValue("@skip", NpgsqlDbType.Integer, skip);
                command.Parameters.AddWithValue("@take", NpgsqlDbType.Integer, take);

                result = new List<KeyValuePair<Guid, Cuboid>>(take);

                using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
                {
                    while (reader.Read() == true)
                    {
                        Cuboid cuboid = new Cuboid
                        {
                            Id = await reader.GetFieldValueAsync<Guid>("id"),
                            Width = reader.GetInt32("width"),
                            Height = reader.GetInt32("height"),
                            Depth = reader.GetInt32("depth")
                        };

                        KeyValuePair<Guid, Cuboid> item = new KeyValuePair<Guid, Cuboid>(cuboid.Id, cuboid);

                        result.Add(item);
                    }
                }
            }

            return result;
        }
    }
}
