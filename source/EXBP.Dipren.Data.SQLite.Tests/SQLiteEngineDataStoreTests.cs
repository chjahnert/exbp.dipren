
using System.Data.SQLite;

using EXBP.Dipren.Tests.Data;

using NUnit.Framework;


namespace EXBP.Dipren.Data.SQLite.Tests
{
    [TestFixture]
    public class SQLiteEngineDataStoreTests : EngineDataStoreTests
    {
        private const string DATABASE_CONNECTION_STRING = "Data Source = :memory:; DateTimeKind = Utc;";

        protected override async Task<IEngineDataStore> OnCreateEngineDataStoreAsync()
            => await SQLiteEngineDataStore.OpenAsync(DATABASE_CONNECTION_STRING, CancellationToken.None);


        [Test]
        public void OpenAsync_ArgumentConnectionIsNull_ThrowsException()
        {
            SQLiteConnection connection = null;

            Assert.ThrowsAsync<ArgumentNullException>(async () => await SQLiteEngineDataStore.OpenAsync(connection, CancellationToken.None));
        }

        [Test]
        public async Task OpenAsync_ConnectionIsClosed_OpensConnection()
        {
            using SQLiteConnection connection = new SQLiteConnection(DATABASE_CONNECTION_STRING);

            SQLiteEngineDataStore store = await SQLiteEngineDataStore.OpenAsync(connection, CancellationToken.None);

            long jobs = await store.CountJobsAsync(CancellationToken.None);
        }

        [Test]
        public async Task OpenAsync_ConnectionIsOpen_UsesConnection()
        {
            using SQLiteConnection connection = new SQLiteConnection(DATABASE_CONNECTION_STRING);

            await connection.OpenAsync();

            SQLiteEngineDataStore store = await SQLiteEngineDataStore.OpenAsync(connection, CancellationToken.None);

            long jobs = await store.CountJobsAsync(CancellationToken.None);
        }

        [Test]
        public async Task OpenAsync_ConnectionIsOpenAndClosed_OpensConnection()
        {
            using SQLiteConnection connection = new SQLiteConnection(DATABASE_CONNECTION_STRING);

            await connection.OpenAsync();
            await connection.CloseAsync();

            SQLiteEngineDataStore store = await SQLiteEngineDataStore.OpenAsync(connection, CancellationToken.None);

            long jobs = await store.CountJobsAsync(CancellationToken.None);
        }
    }
}
