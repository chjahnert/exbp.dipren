﻿
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

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Job job = new Job(id, "Dummy", timestamp, timestamp, JobState.Initializing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Job retrieved = store.Jobs.First(j => j.Id == id);

            Assert.That(retrieved, Is.EqualTo(job));
        }

        [Test]
        public async Task InsertJobAsync_JobWithSameIdentifierAlreadyExists_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Job first = new Job(id, "Dummy", timestamp, timestamp, JobState.Initializing);

            await store.InsertJobAsync(first, CancellationToken.None);

            Job second = first with { };

            Assert.ThrowsAsync<DuplicateIdentifierException>(() => store.InsertJobAsync(second, CancellationToken.None));
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

            Guid jobId = Guid.NewGuid();
            Guid partitionId = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Partition partition = new Partition(partitionId, jobId, "1", timestamp, timestamp, "a", "z", true, "g", 7L, 18L);

            Assert.ThrowsAsync<InvalidReferenceException>(() => store.InsertPartitionAsync(partition, CancellationToken.None));
        }

        [Test]
        public async Task InsertPartitionAsync_PartitionWithSameIdentifierAlreadyExists_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Guid jobId = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;
            Job job = new Job(jobId, "Dummy", timestamp, timestamp, JobState.Initializing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            Partition first = new Partition(partitionId, jobId, "1", timestamp, timestamp, "a", "z", true, "g", 7L, 18L);

            await store.InsertPartitionAsync(first, CancellationToken.None);

            Partition second = first with { };

            Assert.ThrowsAsync<DuplicateIdentifierException>(() => store.InsertPartitionAsync(second, CancellationToken.None));
        }

        [Test]
        public async Task InsertPartitionAsync_ArgumentPartitionIsValid_InsertsPartition()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Guid jobId = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;
            Job job = new Job(jobId, "Dummy", timestamp, timestamp, JobState.Initializing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            Partition partition = new Partition(partitionId, jobId, "1", timestamp, timestamp, "a", "z", true, "g", 7L, 18L);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            Partition retrieved = store.Partitions.First(p => p.Id == partitionId);

            Assert.That(retrieved, Is.EqualTo(partition));
        }

    }
}