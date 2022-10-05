
using EXBP.Dipren.Data;
using EXBP.Dipren.Data.Memory;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Data.Memory
{
    [TestFixture]
    public class InMemoryEngineDataStoreTests
    {
        private readonly Random _random = new Random(19827364);


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

        [Test]
        public void TryAcquirePartitionsAsync_ArgumentJobIdIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            DateTime now = DateTime.UtcNow;
            DateTime cut = now.AddMinutes(-2);

            Assert.ThrowsAsync<ArgumentNullException>(() => store.TryAcquirePartitionsAsync(null, "owner", now, cut, CancellationToken.None));
        }

        [Test]
        public void TryAcquirePartitionsAsync_ArgumentRequesterIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            DateTime now = DateTime.UtcNow;
            DateTime cut = now.AddMinutes(-2);

            Assert.ThrowsAsync<ArgumentNullException>(() => store.TryAcquirePartitionsAsync("DPJ-0001", null, now, cut, CancellationToken.None));
        }

        [Test]
        public void TryAcquirePartitionsAsync_SpecifiedJobDoesNotExist_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            DateTime now = DateTime.UtcNow;
            DateTime cut = now.AddMinutes(-2);

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.TryAcquirePartitionsAsync("DPJ-0001", "owner", now, cut, CancellationToken.None));
        }

        [Test]
        public async Task TryAcquirePartitionsAsync_NoPartitionsExist_ReturnsNull()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime now = DateTime.UtcNow;
            Job job = new Job(jobId, now, now, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            DateTime cut = now.AddMinutes(-2);

            Partition partition = await store.TryAcquirePartitionsAsync(jobId, "owner", now, cut, CancellationToken.None);

            Assert.That(partition, Is.Null);
        }

        [Test]
        public async Task TryAcquirePartitionsAsync_OnlyActivePartitionsExist_ReturnsNull()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "b", 1L, 23L, "owner", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime now = new DateTime(2022, 9, 12, 16, 40, 0, DateTimeKind.Utc);
            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 30, DateTimeKind.Utc);

            Partition acquired = await store.TryAcquirePartitionsAsync(jobId, "owner", now, cut, CancellationToken.None);

            Assert.That(acquired, Is.Null);
        }

        [Test]
        public async Task TryAcquirePartitionsAsync_OnlyCompletedPartitionsExist_ReturnsNull()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "z", 24L, 0L, null, false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime now = new DateTime(2022, 9, 12, 16, 40, 0, DateTimeKind.Utc);
            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 32, DateTimeKind.Utc);

            Partition acquired = await store.TryAcquirePartitionsAsync(jobId, "owner", now, cut, CancellationToken.None);

            Assert.That(acquired, Is.Null);
        }

        [Test]
        public async Task TryAcquirePartitionsAsync_FreePartitionsExist_ReturnsPartition()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "b", 1L, 23L, null, false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime now = new DateTime(2022, 9, 12, 16, 40, 0, DateTimeKind.Utc);
            DateTime cut = new DateTime(2022, 9, 12, 16, 22, 10, DateTimeKind.Utc);

            Partition acquired = await store.TryAcquirePartitionsAsync(jobId, "owner", now, cut, CancellationToken.None);

            Assert.That(acquired, Is.Not.Null);
            Assert.That(acquired.Owner, Is.EqualTo("owner"));
            Assert.That(acquired.Updated, Is.EqualTo(now));
        }

        [Test]
        public async Task TryAcquirePartitionsAsync_AbandonedPartitionsExist_ReturnsPartition()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "b", 1L, 23L, "other", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime now = new DateTime(2022, 9, 12, 16, 40, 0, DateTimeKind.Utc);
            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 32, DateTimeKind.Utc);

            Partition acquired = await store.TryAcquirePartitionsAsync(jobId, "owner", now, cut, CancellationToken.None);

            Assert.That(acquired, Is.Not.Null);
            Assert.That(acquired.Owner, Is.EqualTo("owner"));
            Assert.That(acquired.Updated, Is.EqualTo(now));
        }

        [Test]
        public void TryRequestSplitAsync_ArgumentJobIdIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            DateTime cut = DateTime.UtcNow.AddMinutes(-2);

            Assert.ThrowsAsync<ArgumentNullException>(() => store.TryRequestSplitAsync(null, cut, CancellationToken.None));
        }

        [Test]
        public void TryRequestSplitAsync_SpecifiedJobDoesNotExist_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            DateTime now = DateTime.UtcNow;
            DateTime cut = now.AddMinutes(-2);

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.TryRequestSplitAsync("DPJ-0001", cut, CancellationToken.None));
        }

        [Test]
        public async Task TryRequestSplitAsync_NoPartitionsExist_ReturnsFalse()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime now = DateTime.UtcNow;
            Job job = new Job(jobId, now, now, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            DateTime cut = now.AddMinutes(-2);

            bool result = await store.TryRequestSplitAsync(jobId, cut, CancellationToken.None);

            Assert.That(result, Is.False);
        }

        [Test]
        public async Task TryRequestSplitAsync_OnlyFreePartitionsExist_ReturnsFalse()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "a", 0L, 24L, null, false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 30, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(jobId, cut, CancellationToken.None);

            Assert.That(result, Is.False);
        }

        [Test]
        public async Task TryRequestSplitAsync_OnlyAbandonedPartitionsExist_ReturnsFalse()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 30, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "b", 1L, 23L, "owner", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(jobId, cut, CancellationToken.None);

            Assert.That(result, Is.False);
        }

        [Test]
        public async Task TryRequestSplitAsync_OnlyCompletedPartitionsExist_ReturnsFalse()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "z", 24L, 0L, "owner", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 30, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(jobId, cut, CancellationToken.None);

            Assert.That(result, Is.False);
        }

        [Test]
        public async Task TryRequestSplitAsync_OnlyPartitionsWithSplitRequestExist_ReturnsFalse()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "c", 2L, 22L, "owner", true);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 30, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(jobId, cut, CancellationToken.None);

            Assert.That(result, Is.False);
        }

        [Test]
        public async Task TryRequestSplitAsync_ActivePartitionsExist_ReturnsTrue()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "c", 2L, 22L, "owner", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 30, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(jobId, cut, CancellationToken.None);

            Assert.That(result, Is.True);

            Partition updated = store.Partitions.First(p => p.Id == partitionId);

            Assert.That(updated.IsSplitRequested, Is.True);
        }

        [Test]
        public void ReportProgressAsync_ArgumentOwnerIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.ReportProgressAsync(id, null, timestamp, "d", 4, CancellationToken.None));
        }

        [Test]
        public void ReportProgressAsync_ArgumentPositionIsNull_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.ReportProgressAsync(id, "owner", timestamp, null, 4, CancellationToken.None));
        }

        [Test]
        public void ReportProgressAsync_SpecifiedPartitionDoesNotExist_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.ReportProgressAsync(id, "owner", timestamp, "d", 4, CancellationToken.None));
        }

        [Test]
        public async Task ReportProgressAsync_PartitionLockNoLongerOwned_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "c", 2L, 22L, "other", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime progressUpdated = new DateTime(2022, 9, 12, 16, 26, 11, DateTimeKind.Utc);

            Assert.ThrowsAsync<LockException>(() => store.ReportProgressAsync(partitionId, "owner", progressUpdated, "g", 3, CancellationToken.None));
        }

        [Test]
        public async Task ReportProgressAsync_ValidArguments_UpdatesPartition()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "c", 2L, 22L, "owner", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime progressUpdated = new DateTime(2022, 9, 12, 16, 26, 11, DateTimeKind.Utc);

            await store.ReportProgressAsync(partitionId, "owner", progressUpdated, "g", 3L, CancellationToken.None);

            Partition persisted = store.Partitions.First(p => p.Id == partitionId);

            Assert.That(persisted.JobId, Is.EqualTo(partition.JobId));
            Assert.That(persisted.Created, Is.EqualTo(partition.Created));
            Assert.That(persisted.Updated, Is.EqualTo(progressUpdated));
            Assert.That(persisted.Owner, Is.EqualTo("owner"));
            Assert.That(persisted.First, Is.EqualTo(partition.First));
            Assert.That(persisted.Last, Is.EqualTo(partition.Last));
            Assert.That(persisted.IsInclusive, Is.EqualTo(partition.IsInclusive));
            Assert.That(persisted.Position, Is.EqualTo("g"));
            Assert.That(persisted.Processed, Is.EqualTo(partition.Processed + 3L));
            Assert.That(persisted.Remaining, Is.EqualTo(partition.Remaining - 3L));
            Assert.That(persisted.IsSplitRequested, Is.EqualTo(partition.IsSplitRequested));
        }

        [Test]
        public async Task ReportProgressAsync_ValidArguments_ReturnsUpdatedPartition()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "c", 2L, 22L, "owner", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime progressUpdated = new DateTime(2022, 9, 12, 16, 26, 11, DateTimeKind.Utc);

            Partition persisted = await store.ReportProgressAsync(partitionId, "owner", progressUpdated, "g", 3L, CancellationToken.None);

            Assert.That(persisted.JobId, Is.EqualTo(partition.JobId));
            Assert.That(persisted.Created, Is.EqualTo(partition.Created));
            Assert.That(persisted.Updated, Is.EqualTo(progressUpdated));
            Assert.That(persisted.Owner, Is.EqualTo("owner"));
            Assert.That(persisted.First, Is.EqualTo(partition.First));
            Assert.That(persisted.Last, Is.EqualTo(partition.Last));
            Assert.That(persisted.IsInclusive, Is.EqualTo(partition.IsInclusive));
            Assert.That(persisted.Position, Is.EqualTo("g"));
            Assert.That(persisted.Processed, Is.EqualTo(partition.Processed + 3L));
            Assert.That(persisted.Remaining, Is.EqualTo(partition.Remaining - 3L));
            Assert.That(persisted.IsSplitRequested, Is.EqualTo(partition.IsSplitRequested));
        }
    }
}
