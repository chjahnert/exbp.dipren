
using NUnit.Framework;


namespace EXBP.Dipren.Data.Postgres.Tests
{
    [TestFixture]
    public class ResilientPostgresEngineDataStoreTests : PostgresEngineDataStoreTests
    {
        private const int DEFAULT_RETRY_LIMIT = 0;
        private const int DEFAULT_RETRY_DELAY = 20;

        private TimeSpan DefaultRetryDely => TimeSpan.FromMilliseconds(DEFAULT_RETRY_DELAY);

        protected override Task<IEngineDataStore> OnCreateEngineDataStoreAsync()
            => Task.FromResult<IEngineDataStore>(new ResilientPostgresEngineDataStore(CONNECTION_STRING, DEFAULT_RETRY_LIMIT, this.DefaultRetryDely));
    }
}
