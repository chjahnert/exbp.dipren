
using EXBP.Dipren.Data.Tests;

using NUnit.Framework;


namespace EXBP.Dipren.Data.SQLite.Tests
{
    [TestFixture]
    public class SQLiteEngineDataStoreTests : EngineDataStoreTests
    {
        protected const string CONNECTION_STRING = "Data Source = :memory:; DateTimeKind = Utc;";

        protected override Task<IEngineDataStore> OnCreateEngineDataStoreAsync()
            => Task.FromResult<IEngineDataStore>(new SQLiteEngineDataStore(CONNECTION_STRING));
    }
}
