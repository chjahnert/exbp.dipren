
using EXBP.Dipren.Tests.Data;

using NUnit.Framework;


namespace EXBP.Dipren.Data.SQLite.Tests
{
    [TestFixture]
    public class SQLiteEngineDataStoreTests : EngineDataStoreTests
    {
        private const string DATABASE_CONNECTION_STRING = "Data Source = :memory:; DateTimeKind = Utc;";

        protected override Task<IEngineDataStore> OnCreateEngineDataStoreAsync()
            => Task.FromResult<IEngineDataStore>(new SQLiteEngineDataStore(DATABASE_CONNECTION_STRING));
    }
}
