
using EXBP.Dipren.Data;
using EXBP.Dipren.Data.Memory;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Data.Memory
{
    [TestFixture]
    public class InMemoryEngineDataStoreTests
    {
        [Test]
        public void InsertAsync_ArgumentJobIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();
            Job job = null;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.InsertAsync(job, CancellationToken.None));
        }

        [Test]
        public async Task InsertAsync_ArgumentJobIsValid_InsertsJob()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Job job = new Job(id, "Dummy", timestamp, timestamp, JobState.Initializing);

            await store.InsertAsync(job, CancellationToken.None);

            Job retrieved = store.Jobs.First(j => j.Id == id);

            Assert.That(retrieved, Is.EqualTo(job));
        }

        [Test]
        public async Task InsertAsync_JobWithSameIdentifierAlreadyExists_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Job first = new Job(id, "Dummy", timestamp, timestamp, JobState.Initializing);

            await store.InsertAsync(first, CancellationToken.None);

            Job second = first with { };

            Assert.ThrowsAsync<DuplicateIdentifierException>(() => store.InsertAsync(second, CancellationToken.None));
        }

        [Test]
        public void InsertAsync_ArgumentPartitionIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();
            Partition partition = null;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.InsertAsync(partition, CancellationToken.None));
        }

        [Test]
        public void InsertAsync_ReferencedJobDoesNotExist_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Guid jobId = Guid.NewGuid();
            Guid partitionId = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Partition partition = new Partition(partitionId, jobId, "1", timestamp, timestamp, "a", "z", true, "g", 7L, 18L);

            Assert.ThrowsAsync<InvalidReferenceException>(() => store.InsertAsync(partition, CancellationToken.None));
        }

        [Test]
        public async Task InsertAsync_PartitionWithSameIdentifierAlreadyExists_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Guid jobId = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;
            Job job = new Job(jobId, "Dummy", timestamp, timestamp, JobState.Initializing);

            await store.InsertAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            Partition first = new Partition(partitionId, jobId, "1", timestamp, timestamp, "a", "z", true, "g", 7L, 18L);

            await store.InsertAsync(first, CancellationToken.None);

            Partition second = first with { };

            Assert.ThrowsAsync<DuplicateIdentifierException>(() => store.InsertAsync(second, CancellationToken.None));
        }

        [Test]
        public async Task InsertAsync_ArgumentPartitionIsValid_InsertsPartition()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Guid jobId = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;
            Job job = new Job(jobId, "Dummy", timestamp, timestamp, JobState.Initializing);

            await store.InsertAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            Partition partition = new Partition(partitionId, jobId, "1", timestamp, timestamp, "a", "z", true, "g", 7L, 18L);

            await store.InsertAsync(partition, CancellationToken.None);

            Partition retrieved = store.Partitions.First(p => p.Id == partitionId);

            Assert.That(retrieved, Is.EqualTo(partition));
        }

    }
}
