
using System.Diagnostics;

using Npgsql;
using NpgsqlTypes;

using EXBP.Dipren.Demo.Postgres.Processing.Models;


namespace EXBP.Dipren.Demo.Postgres.Processing
{
    internal class CubiodBatchProcessor : IBatchProcessor<Cuboid>
    {
        private const string SQL_STATE_PRIMARY_KEY_VIOLATION = "23505";

        private readonly string _connectionString;

        internal CubiodBatchProcessor(string connectionString)
        {
            Debug.Assert(connectionString != null);

            this._connectionString = connectionString;
        }

        public async Task ProcessAsync(IEnumerable<Cuboid> cuboids, CancellationToken cancellation)
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(this._connectionString))
            {
                await connection.OpenAsync(cancellation);

                using (NpgsqlBinaryImporter writer = await connection.BeginBinaryImportAsync("COPY \"demo\".\"volumes\" ( \"id\", \"volume\" ) FROM STDIN (FORMAT BINARY)", cancellation))
                {
                    foreach(Cuboid cuboid in cuboids)
                    {
                        await writer.StartRowAsync(cancellation);
                        await writer.WriteAsync(cuboid.Id, NpgsqlDbType.Uuid, cancellation);
                        await writer.WriteAsync((cuboid.Width * cuboid.Height * cuboid.Depth), NpgsqlDbType.Integer, cancellation);
                    }

                    try
                    {
                        await writer.CompleteAsync();
                    }
                    catch (PostgresException ex) when (ex.SqlState == SQL_STATE_PRIMARY_KEY_VIOLATION)
                    {
                        //
                        // In case a processing node crashed or was terminated before it had a chance to register the
                        // progress it has made, the last batch may be reprocessed in which case we could end up with
                        // a primary key violation error.
                        //

                        Debug.WriteLine("PRIMARY KEY VIOLATION");
                    }
                }
            }
        }
    }
}
