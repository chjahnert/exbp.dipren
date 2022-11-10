
using System.Diagnostics;

using Npgsql;
using NpgsqlTypes;

using EXBP.Dipren.Demo.Postgres.Processing.Models;


namespace EXBP.Dipren.Demo.Postgres.Processing
{
    internal class CubiodBatchProcessor : IBatchProcessor<Cuboid>
    {
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

                    await writer.CompleteAsync();
                }
            }
        }
    }
}
