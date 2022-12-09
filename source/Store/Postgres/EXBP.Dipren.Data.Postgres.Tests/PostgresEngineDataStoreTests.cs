
using EXBP.Dipren.Data.Tests;

using Npgsql;

using NUnit.Framework;


namespace EXBP.Dipren.Data.Postgres.Tests
{
    [TestFixture]
    public class PostgresEngineDataStoreTests : EngineDataStoreTests, IDisposable
    {
        protected const string CONNECTION_STRING = "Host = localhost; Port = 5432; Database = postgres; User ID = postgres; Password = development";
        private const string PATH_SCHEMA_SCRIPT = @"../../../../Database/dipren.sql";


        protected NpgsqlDataSource DataSource { get; }


        public PostgresEngineDataStoreTests()
        {
            NpgsqlDataSourceBuilder builder = new NpgsqlDataSourceBuilder(CONNECTION_STRING);

            this.DataSource = builder.Build();
        }


        public void Dispose()
        {
            this.DataSource.Dispose();
        }


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

        private async Task DropDatabaseSchemaAsync(CancellationToken cancellation)
        {
            await using NpgsqlCommand command = this.DataSource.CreateCommand(PostgresEngineDataStoreTestsResources.QueryDropSchema);

            await command.ExecuteNonQueryAsync(cancellation);
        }

        private async Task CreateDatabaseSchemaAsync(CancellationToken cancellation)
        {
            string script = await File.ReadAllTextAsync(PATH_SCHEMA_SCRIPT, cancellation);

            await using NpgsqlCommand command = this.DataSource.CreateCommand(script);

            await command.ExecuteNonQueryAsync(cancellation);
        }
    }
}
