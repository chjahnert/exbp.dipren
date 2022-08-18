
using EXBP.Dipren.Data;
using EXBP.Dipren.Data.Memory;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Data.Memory
{
    [TestFixture]
    public class InMemoryEngineDataStoreTests
    {
        [Test]
        public void AddAsync_ArgumentJobIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();
            Job job = null;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.InsertAsync(job, CancellationToken.None));
        }

        [Test]
        public async Task AddAsync_ArgumentJobIsValid_InsertsJob()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Job job = new Job(id, "Dummy", timestamp, timestamp, JobState.Initializing);

            await store.InsertAsync(job, CancellationToken.None);

            long count = await store.CountJobsAsync(CancellationToken.None);

            Assert.That(count, Is.EqualTo(1L));
        }

        [Test]
        public async Task AddAsync_JobWithSameIdentifierAlreadyExists_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Job first = new Job(id, "Dummy", timestamp, timestamp, JobState.Initializing);

            await store.InsertAsync(first, CancellationToken.None);

            Job second = first with { };

            Assert.ThrowsAsync<DuplicateIdentifierException>(() => store.InsertAsync(second, CancellationToken.None));
        }
    }
}
