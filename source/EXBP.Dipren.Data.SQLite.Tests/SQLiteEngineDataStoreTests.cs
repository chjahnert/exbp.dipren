
using EXBP.Dipren.Tests.Data;

using NUnit.Framework;


namespace EXBP.Dipren.Data.SQLite.Tests
{
    [TestFixture]
    public class SQLiteEngineDataStoreTests : EngineDataStoreTests
    {
        protected override async Task<IEngineDataStore> OnCreateEngineDataStoreAsync()
            => await SQLiteEngineDataStore.OpenAsync("Data Source = :memory:; DateTimeKind = Utc;", CancellationToken.None);
    }
}
