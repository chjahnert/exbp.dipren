
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class InMemoryEngineDataStoreTests
    {
        [Test]
        public void InsertJobAsync_ArgumentNameIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Assert.ThrowsAsync<ArgumentNullException>(async () => await store.InsertJobAsync(null, JobState.Initializing, CancellationToken.None));
        }

        [Test]
        public async Task InsertJobAsync_ArgumentsAreValid_InsertsJob()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            await store.InsertJobAsync("first", JobState.Initializing, CancellationToken.None);

            long count = await store.CountJobsAsync(CancellationToken.None);

            Assert.That(count, Is.EqualTo(1L));
        }

        [Test]
        public async Task InsertJobAsync_JobWithSameNameAreadyExists_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            await store.InsertJobAsync("first", JobState.Initializing, CancellationToken.None);

            Assert.ThrowsAsync<ArgumentException>(async () => await store.InsertJobAsync("first", JobState.Initializing, CancellationToken.None));
        }
    }
}
