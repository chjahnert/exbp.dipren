
using EXBP.Dipren.Data;
using EXBP.Dipren.Data.Memory;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Data.Memory
{
    [TestFixture]
    public class InMemoryEngineDataStoreTests
    {
        [Test]
        public void InsertJobAsync_ArgumentJobIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();
            Job job = null;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.InsertJobAsync(job, CancellationToken.None));
        }

        [Test]
        public async Task InsertJobAsync_ArgumentJobIsValid_InsertsJob()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime timestamp = DateTime.UtcNow;

            Job job = new Job(jobId, timestamp, timestamp, JobState.Initializing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Job retrieved = store.Jobs.First(j => j.Id == jobId);

            Assert.That(retrieved, Is.EqualTo(job));
        }

        [Test]
        public async Task InsertJobAsync_JobWithSameIdentifierAlreadyExists_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string id = "DPJ-0001";
            DateTime timestamp = DateTime.UtcNow;

            Job first = new Job(id, timestamp, timestamp, JobState.Initializing);

            await store.InsertJobAsync(first, CancellationToken.None);

            Job second = first with { };

            Assert.ThrowsAsync<DuplicateIdentifierException>(() => store.InsertJobAsync(second, CancellationToken.None));
        }

        [Test]
        public void RetirveJobAsync_ArgumentIdIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Assert.ThrowsAsync<ArgumentNullException>(() => store.RetrieveJobAsync(null, CancellationToken.None));
        }

        [Test]
        public void RetirveJobAsync_JobDoesNotExist_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.RetrieveJobAsync("DPJ-0001", CancellationToken.None));
        }

        [Test]
        public async Task RetirveJobAsync_JobExist_ReturnsJob()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string id = "DPJ-0001";
            DateTime timestamp = DateTime.UtcNow;

            Job inserted = new Job(id, timestamp, timestamp, JobState.Initializing);

            await store.InsertJobAsync(inserted, CancellationToken.None);

            Job retrieved = await store.RetrieveJobAsync("DPJ-0001", CancellationToken.None);

            Assert.That(retrieved, Is.Not.Null);
            Assert.That(retrieved.Id, Is.EqualTo(id));
            Assert.That(retrieved.Created, Is.EqualTo(timestamp));
            Assert.That(retrieved.Updated, Is.EqualTo(timestamp));
            Assert.That(retrieved.State, Is.EqualTo(JobState.Initializing));
            Assert.That(retrieved.Exception, Is.Null);
        }

        [Test]
        public void InsertPartitionAsync_ArgumentPartitionIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();
            Partition partition = null;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.InsertPartitionAsync(partition, CancellationToken.None));
        }

        [Test]
        public void InsertPartitionAsync_ReferencedJobDoesNotExist_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            Guid partitionId = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Partition partition = new Partition(partitionId, jobId, timestamp, timestamp, "a", "z", true, "g", 7L, 18L);

            Assert.ThrowsAsync<InvalidReferenceException>(() => store.InsertPartitionAsync(partition, CancellationToken.None));
        }

        [Test]
        public async Task InsertPartitionAsync_PartitionWithSameIdentifierAlreadyExists_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime timestamp = DateTime.UtcNow;
            Job job = new Job(jobId, timestamp, timestamp, JobState.Initializing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            Partition first = new Partition(partitionId, jobId, timestamp, timestamp, "a", "z", true, "g", 7L, 18L);

            await store.InsertPartitionAsync(first, CancellationToken.None);

            Partition second = first with { };

            Assert.ThrowsAsync<DuplicateIdentifierException>(() => store.InsertPartitionAsync(second, CancellationToken.None));
        }

        [Test]
        public async Task InsertPartitionAsync_ArgumentPartitionIsValid_InsertsPartition()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime timestamp = DateTime.UtcNow;
            Job job = new Job(jobId, timestamp, timestamp, JobState.Initializing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            Partition partition = new Partition(partitionId, jobId, timestamp, timestamp, "a", "z", true, "g", 7L, 18L);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            Partition retrieved = store.Partitions.First(p => p.Id == partitionId);

            Assert.That(retrieved, Is.EqualTo(partition));
        }

    }
}
