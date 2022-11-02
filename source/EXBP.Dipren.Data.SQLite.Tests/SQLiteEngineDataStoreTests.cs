
using EXBP.Dipren.Data.Memory;
using EXBP.Dipren.Tests.Data.Memory;

using NUnit.Framework;


namespace EXBP.Dipren.Data.SQLite.Tests
{
    [TestFixture]
    public class SQLiteEngineDataStoreTests : EngineDataStoreTests<SQLiteEngineDataStore>
    {
        protected override async Task<SQLiteEngineDataStore> CreateEngineDataStoreAsync()
            => await SQLiteEngineDataStore.OpenAsync("Data Source = :memory:; DateTimeKind = Utc;", CancellationToken.None);
    }
}
