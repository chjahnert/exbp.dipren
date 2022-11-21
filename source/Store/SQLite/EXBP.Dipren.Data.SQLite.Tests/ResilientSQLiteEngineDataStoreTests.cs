
using NUnit.Framework;


namespace EXBP.Dipren.Data.SQLite.Tests
{
    [TestFixture]
    public class ResilientSQLiteEngineDataStoreTests : SQLiteEngineDataStoreTests
    {
        private const int DEFAULT_RETRY_LIMIT = 0;
        private const int DEFAULT_RETRY_DELAY = 20;


        private TimeSpan DefaultRetryDely => TimeSpan.FromMilliseconds(DEFAULT_RETRY_DELAY);


        protected override Task<IEngineDataStore> OnCreateEngineDataStoreAsync()
            => Task.FromResult<IEngineDataStore>(new ResilientSQLiteEngineDataStore(CONNECTION_STRING, DEFAULT_RETRY_LIMIT, this.DefaultRetryDely));
    }
}
