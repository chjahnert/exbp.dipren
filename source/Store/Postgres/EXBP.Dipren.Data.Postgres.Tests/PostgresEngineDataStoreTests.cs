
using Npgsql;

using EXBP.Dipren.Tests.Data;
using System.Data;

using NUnit.Framework;


namespace EXBP.Dipren.Data.Postgres.Tests
{
    [TestFixture]
    public class PostgresEngineDataStoreTests : EngineDataStoreTests
    {
        private const string CONNECTION_STRING = "Host = localhost; Port = 5432; Database = postgres; User ID = postgres; Password = development";
        private const string PATH_SCHEMA_SCRIPT = @"../../../../Database/dipren.sql";


        private string ConnectionString { get; } = CONNECTION_STRING;


        protected override Task<IEngineDataStore> OnCreateEngineDataStoreAsync()
            => Task.FromResult<IEngineDataStore>(new PostgresEngineDataStore(CONNECTION_STRING));

        protected override DateTime FormatDateTime(DateTime source)
            => new DateTime(source.Ticks - (source.Ticks % (TimeSpan.TicksPerMillisecond / 1000)), source.Kind);


        [SetUp]
        public async Task BeforeEachTestCaseAsync()
        {
            await this.DropDatabaseSchemaAsync(CancellationToken.None);
            await this.CreateDatabaseSchemaAsync(CancellationToken.None);
        }

        [OneTimeTearDown]
        public async Task AfterTestFixtureAsync()
        {
            await this.DropDatabaseSchemaAsync(CancellationToken.None);
        }

        private async Task<NpgsqlConnection> OpenDatabaseConnectionAsync(CancellationToken cancellation)
        {
            NpgsqlConnection result = new NpgsqlConnection(this.ConnectionString);

            await result.OpenAsync(cancellation);

            return result;
        }

        private async Task DropDatabaseSchemaAsync(CancellationToken cancellation)
        {
            using (NpgsqlConnection connection = await this.OpenDatabaseConnectionAsync(cancellation))
            {
                using NpgsqlCommand command = new NpgsqlCommand
                {
                    CommandText = PostgresEngineDataStoreTestsResources.QueryDropSchema,
                    CommandType = CommandType.Text,
                    Connection = connection
                };

                await command.ExecuteNonQueryAsync(cancellation);
            }
        }

        private async Task CreateDatabaseSchemaAsync(CancellationToken cancellation)
        {
            using (NpgsqlConnection connection = await this.OpenDatabaseConnectionAsync(cancellation))
            {
                using NpgsqlCommand command = new NpgsqlCommand
                {
                    CommandText = await File.ReadAllTextAsync(PATH_SCHEMA_SCRIPT, cancellation),
                    CommandType = CommandType.Text,
                    Connection = connection
                };

                await command.ExecuteNonQueryAsync(cancellation);
            }
        }
    }
}
