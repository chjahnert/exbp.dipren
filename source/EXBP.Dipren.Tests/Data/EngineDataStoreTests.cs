using EXBP.Dipren.Data;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Data
{
    public abstract class EngineDataStoreTests<TStore> where TStore : IEngineDataStore
    {
        protected abstract Task<TStore> CreateEngineDataStoreAsync();


        [Test]
        public async Task CountJobsAsync_NoJobsAvailable_RetrunsZero()
        {
            TStore store = await CreateEngineDataStoreAsync();

            long result = await store.CountJobsAsync(CancellationToken.None);

            Assert.That(result, Is.Zero);
        }

        [Test]
        public async Task CountJobsAsync_MultipleJobsAvailable_RetrunsCorrectCount()
        {
            TStore store = await CreateEngineDataStoreAsync();

            DateTime timestamp = new DateTime(2022, 9, 11, 11, 23, 7, DateTimeKind.Utc);

            Job job1 = new Job("DPJ-0001", timestamp, timestamp, JobState.Initializing);
            Job job2 = new Job("DPJ-0002", timestamp, timestamp, JobState.Ready);
            Job job3 = new Job("DPJ-0003", timestamp, timestamp, JobState.Processing);
            Job job4 = new Job("DPJ-0004", timestamp, timestamp, JobState.Completed);
            Job job5 = new Job("DPJ-0005", timestamp, timestamp, JobState.Failed);

            await store.InsertJobAsync(job1, CancellationToken.None);
            await store.InsertJobAsync(job2, CancellationToken.None);
            await store.InsertJobAsync(job3, CancellationToken.None);
            await store.InsertJobAsync(job4, CancellationToken.None);
            await store.InsertJobAsync(job5, CancellationToken.None);

            long result = await store.CountJobsAsync(CancellationToken.None);

            Assert.That(result, Is.EqualTo(5L));
        }


        [Test]
        public async Task InsertJobAsync_ArgumentJobIsNull_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            Job job = null;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.InsertJobAsync(job, CancellationToken.None));
        }

        [Test]
        public async Task InsertJobAsync_ArgumentJobIsValid_InsertsJob()
        {
            TStore store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            DateTime timestamp = DateTime.UtcNow;

            Job job = new Job(jobId, timestamp, timestamp, JobState.Initializing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Job retrieved = await store.RetrieveJobAsync(jobId, CancellationToken.None);

            Assert.That(retrieved, Is.EqualTo(job));
        }

        [Test]
        public async Task InsertJobAsync_JobWithSameIdentifierAlreadyExists_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            const string id = "DPJ-0001";
            DateTime timestamp = DateTime.UtcNow;

            Job first = new Job(id, timestamp, timestamp, JobState.Initializing);

            await store.InsertJobAsync(first, CancellationToken.None);

            Job second = first with { };

            Assert.ThrowsAsync<DuplicateIdentifierException>(() => store.InsertJobAsync(second, CancellationToken.None));
        }

        [Test]
        public async Task UpdateJobAsync_ArgumentJoIdIsNull_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<ArgumentNullException>(() => store.UpdateJobAsync(null, DateTime.UtcNow, JobState.Completed, null, CancellationToken.None));
        }

        [Test]
        public async Task UpdateJobAsync_JobDoesNotExist_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.UpdateJobAsync("DPJ-0001", DateTime.UtcNow, JobState.Completed, null, CancellationToken.None));
        }

        [Test]
        public async Task UpdateJobAsync_JobExists_JobIsUpdated()
        {
            TStore store = await CreateEngineDataStoreAsync();

            const string id = "DPJ-0001";
            DateTime created = new DateTime(2022, 9, 11, 11, 6, 1, DateTimeKind.Utc);

            Job job = new Job(id, created, created, JobState.Ready);

            await store.InsertJobAsync(job, CancellationToken.None);

            DateTime updated = new DateTime(2022, 9, 11, 13, 21, 49, DateTimeKind.Utc);
            string error = "The connection to the data source was terminated.";

            await store.UpdateJobAsync(id, updated, JobState.Failed, error, CancellationToken.None);

            Job persisted = await store.RetrieveJobAsync(id, CancellationToken.None);

            Assert.That(persisted.Created, Is.EqualTo(created));
            Assert.That(persisted.Updated, Is.EqualTo(updated));
            Assert.That(persisted.State, Is.EqualTo(JobState.Failed));
            Assert.That(persisted.Error, Is.EqualTo(error));
        }

        [Test]
        public async Task UpdateJobAsync_JobExists_ReturnsUpdatedJob()
        {
            TStore store = await CreateEngineDataStoreAsync();

            const string id = "DPJ-0001";
            DateTime created = new DateTime(2022, 9, 11, 11, 6, 1, DateTimeKind.Utc);

            Job job = new Job(id, created, created, JobState.Ready);

            await store.InsertJobAsync(job, CancellationToken.None);

            DateTime updated = new DateTime(2022, 9, 11, 13, 21, 49, DateTimeKind.Utc);
            string error = "The connection to the data source was terminated.";

            Job persisted = await store.UpdateJobAsync(id, updated, JobState.Failed, error, CancellationToken.None);

            Assert.That(persisted.Created, Is.EqualTo(created));
            Assert.That(persisted.Updated, Is.EqualTo(updated));
            Assert.That(persisted.State, Is.EqualTo(JobState.Failed));
            Assert.That(persisted.Error, Is.EqualTo(error));
        }

        [Test]
        public async Task RetirveJobAsync_ArgumentIdIsNull_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<ArgumentNullException>(() => store.RetrieveJobAsync(null, CancellationToken.None));
        }

        [Test]
        public async Task RetirveJobAsync_JobDoesNotExist_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.RetrieveJobAsync("DPJ-0001", CancellationToken.None));
        }

        [Test]
        public async Task RetirveJobAsync_JobExist_ReturnsJob()
        {
            TStore store = await CreateEngineDataStoreAsync();

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
            Assert.That(retrieved.Error, Is.Null);
        }

        [Test]
        public async Task InsertPartitionAsync_ArgumentPartitionIsNull_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();
            Partition partition = null;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.InsertPartitionAsync(partition, CancellationToken.None));
        }

        [Test]
        public async Task InsertPartitionAsync_ReferencedJobDoesNotExist_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            Guid partitionId = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Partition partition = new Partition(partitionId, jobId, timestamp, timestamp, "a", "z", true, "g", 7L, 18L);

            Assert.ThrowsAsync<InvalidReferenceException>(() => store.InsertPartitionAsync(partition, CancellationToken.None));
        }

        [Test]
        public async Task InsertPartitionAsync_PartitionWithSameIdentifierAlreadyExists_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

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
            TStore store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            DateTime timestamp = DateTime.UtcNow;
            Job job = new Job(jobId, timestamp, timestamp, JobState.Initializing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            Partition partition = new Partition(partitionId, jobId, timestamp, timestamp, "a", "z", true, "g", 7L, 18L);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            Partition retrieved = await store.RetrievePartitionAsync(partitionId, CancellationToken.None);

            Assert.That(retrieved, Is.EqualTo(partition));
        }

        [Test]
        public async Task RetirvePartitionAsync_PartitionDoesNotExist_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            Guid id = Guid.NewGuid();

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.RetrievePartitionAsync(id, CancellationToken.None));
        }

        [Test]
        public async Task RetirvePartitionAsync_PartitionExists_ReturnsPartition()
        {
            TStore store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "c", 2L, 22L, "owner", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            Partition persisted = await store.RetrievePartitionAsync(partitionId, CancellationToken.None);

            Assert.That(persisted, Is.EqualTo(partition));
        }

        [Test]
        public async Task TryAcquirePartitionsAsync_ArgumentJobIdIsNull_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            DateTime now = DateTime.UtcNow;
            DateTime cut = now.AddMinutes(-2);

            Assert.ThrowsAsync<ArgumentNullException>(() => store.TryAcquirePartitionsAsync(null, "owner", now, cut, CancellationToken.None));
        }

        [Test]
        public async Task TryAcquirePartitionsAsync_ArgumentRequesterIsNull_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            DateTime now = DateTime.UtcNow;
            DateTime cut = now.AddMinutes(-2);

            Assert.ThrowsAsync<ArgumentNullException>(() => store.TryAcquirePartitionsAsync("DPJ-0001", null, now, cut, CancellationToken.None));
        }

        [Test]
        public async Task TryAcquirePartitionsAsync_SpecifiedJobDoesNotExist_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            DateTime now = DateTime.UtcNow;
            DateTime cut = now.AddMinutes(-2);

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.TryAcquirePartitionsAsync("DPJ-0001", "owner", now, cut, CancellationToken.None));
        }

        [Test]
        public async Task TryAcquirePartitionsAsync_NoPartitionsExist_ReturnsNull()
        {
            TStore store = await CreateEngineDataStoreAsync();

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
            TStore store = await CreateEngineDataStoreAsync();

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
            TStore store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "z", 24L, 0L, null, true, false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime now = new DateTime(2022, 9, 12, 16, 40, 0, DateTimeKind.Utc);
            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 32, DateTimeKind.Utc);

            Partition acquired = await store.TryAcquirePartitionsAsync(jobId, "owner", now, cut, CancellationToken.None);

            Assert.That(acquired, Is.Null);
        }

        [Test]
        public async Task TryAcquirePartitionsAsync_FreePartitionsExist_ReturnsPartition()
        {
            TStore store = await CreateEngineDataStoreAsync();

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
            TStore store = await CreateEngineDataStoreAsync();

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
        public async Task TryRequestSplitAsync_ArgumentJobIdIsNull_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            DateTime cut = DateTime.UtcNow.AddMinutes(-2);

            Assert.ThrowsAsync<ArgumentNullException>(() => store.TryRequestSplitAsync(null, cut, CancellationToken.None));
        }

        [Test]
        public async Task TryRequestSplitAsync_SpecifiedJobDoesNotExist_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            DateTime now = DateTime.UtcNow;
            DateTime cut = now.AddMinutes(-2);

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.TryRequestSplitAsync("DPJ-0001", cut, CancellationToken.None));
        }

        [Test]
        public async Task TryRequestSplitAsync_NoPartitionsExist_ReturnsFalse()
        {
            TStore store = await CreateEngineDataStoreAsync();

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
            TStore store = await CreateEngineDataStoreAsync();

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
            TStore store = await CreateEngineDataStoreAsync();

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
            TStore store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "z", 24L, 0L, "owner", true);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 30, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(jobId, cut, CancellationToken.None);

            Assert.That(result, Is.False);
        }

        [Test]
        public async Task TryRequestSplitAsync_OnlyPartitionsWithSplitRequestExist_ReturnsFalse()
        {
            TStore store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "c", 2L, 22L, "owner", false, true);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 30, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(jobId, cut, CancellationToken.None);

            Assert.That(result, Is.False);
        }

        [Test]
        public async Task TryRequestSplitAsync_ActivePartitionsExist_ReturnsTrue()
        {
            TStore store = await CreateEngineDataStoreAsync();

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

            Partition updated = await store.RetrievePartitionAsync(partitionId, CancellationToken.None);

            Assert.That(updated.IsSplitRequested, Is.True);
        }

        [Test]
        public async Task ReportProgressAsync_ArgumentOwnerIsNull_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.ReportProgressAsync(id, null, timestamp, "d", 4, false, CancellationToken.None));
        }

        [Test]
        public async Task ReportProgressAsync_ArgumentPositionIsNull_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.ReportProgressAsync(id, "owner", timestamp, null, 4, false, CancellationToken.None));
        }

        [Test]
        public async Task ReportProgressAsync_SpecifiedPartitionDoesNotExist_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.ReportProgressAsync(id, "owner", timestamp, "d", 4, false, CancellationToken.None));
        }

        [Test]
        public async Task ReportProgressAsync_PartitionLockNoLongerOwned_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

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

            Assert.ThrowsAsync<LockException>(() => store.ReportProgressAsync(partitionId, "owner", progressUpdated, "g", 3, false, CancellationToken.None));
        }

        [Test]
        public async Task ReportProgressAsync_ValidArguments_UpdatesPartition()
        {
            TStore store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "c", 2L, 22L, "owner", false, false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime progressUpdated = new DateTime(2022, 9, 12, 16, 26, 11, DateTimeKind.Utc);

            await store.ReportProgressAsync(partitionId, "owner", progressUpdated, "g", 3L, true, CancellationToken.None);

            Partition persisted = await store.RetrievePartitionAsync(partitionId, CancellationToken.None);

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
            Assert.That(persisted.IsCompleted, Is.True);
            Assert.That(persisted.IsSplitRequested, Is.EqualTo(partition.IsSplitRequested));
        }

        [Test]
        public async Task ReportProgressAsync_ValidArguments_ReturnsUpdatedPartition()
        {
            TStore store = await CreateEngineDataStoreAsync();

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

            Partition returned = await store.ReportProgressAsync(partitionId, "owner", progressUpdated, "g", 3L, true, CancellationToken.None);

            Assert.That(returned.JobId, Is.EqualTo(partition.JobId));
            Assert.That(returned.Created, Is.EqualTo(partition.Created));
            Assert.That(returned.Updated, Is.EqualTo(progressUpdated));
            Assert.That(returned.Owner, Is.EqualTo("owner"));
            Assert.That(returned.First, Is.EqualTo(partition.First));
            Assert.That(returned.Last, Is.EqualTo(partition.Last));
            Assert.That(returned.IsInclusive, Is.EqualTo(partition.IsInclusive));
            Assert.That(returned.Position, Is.EqualTo("g"));
            Assert.That(returned.Processed, Is.EqualTo(partition.Processed + 3L));
            Assert.That(returned.Remaining, Is.EqualTo(partition.Remaining - 3L));
            Assert.That(returned.IsCompleted, Is.True);
            Assert.That(returned.IsSplitRequested, Is.EqualTo(partition.IsSplitRequested));
        }

        [Test]
        public async Task InsertSplitPartition_ArgumentPartitionToUpdateIsNull_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "c", 2L, 22L, "owner", false);

            Assert.ThrowsAsync<ArgumentNullException>(() => store.InsertSplitPartitionAsync(null, partition, CancellationToken.None));
        }

        [Test]
        public async Task InsertSplitPartition_ArgumentPartitionToInsertIsNull_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            DateTime timestamp = DateTime.UtcNow;
            Job job = new Job(jobId, timestamp, timestamp, JobState.Initializing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionId = Guid.NewGuid();
            DateTime partitionCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(partitionId, jobId, partitionCreated, partitionUpdated, "a", "z", true, "g", 7L, 18L, "owner", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            Assert.ThrowsAsync<ArgumentNullException>(() => store.InsertSplitPartitionAsync(partition, null, CancellationToken.None));
        }

        [Test]
        public async Task InsertSplitPartition_PartitionToUpdateDoesNotExist_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionToUpdateId = Guid.NewGuid();
            DateTime partitionToUpdateCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionToUpdateUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partitionToUpdate = new Partition(partitionToUpdateId, jobId, partitionToUpdateCreated, partitionToUpdateUpdated, "a", "m", true, "c", 2L, 12L, "owner", false);

            Guid partitionToInsertId = Guid.NewGuid();
            DateTime partitionToInsertCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionToInsertUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partitionToInsert = new Partition(partitionToInsertId, jobId, partitionToInsertCreated, partitionToInsertUpdated, "n", "z", true, null, 0L, 12L, null, false);

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.InsertSplitPartitionAsync(partitionToUpdate, partitionToInsert, CancellationToken.None));
        }

        [Test]
        public async Task InsertSplitPartition_PartitionToInsertAlreadyExists_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionToUpdateId = Guid.NewGuid();
            DateTime partitionToUpdateCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionToUpdateUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partitionToUpdate = new Partition(partitionToUpdateId, jobId, partitionToUpdateCreated, partitionToUpdateUpdated, "a", "m", true, "c", 2L, 12L, "owner", false);

            await store.InsertPartitionAsync(partitionToUpdate, CancellationToken.None);

            Guid partitionToInsertId = Guid.NewGuid();
            DateTime partitionToInsertCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionToInsertUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partitionToInsert = new Partition(partitionToInsertId, jobId, partitionToInsertCreated, partitionToInsertUpdated, "n", "z", true, null, 0L, 12L, null, false);

            await store.InsertPartitionAsync(partitionToInsert, CancellationToken.None);

            Assert.ThrowsAsync<DuplicateIdentifierException>(() => store.InsertSplitPartitionAsync(partitionToUpdate, partitionToInsert, CancellationToken.None));
        }

        [Test]
        public async Task InsertSplitPartition_ArgumensAreValid_UpdatesExistingAndInsertsNewPartition()
        {
            TStore store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid partitionToSplitId = Guid.NewGuid();
            DateTime partitionToSplitCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionToSplitUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partitionToSplit = new Partition(partitionToSplitId, jobId, partitionToSplitCreated, partitionToSplitUpdated, "a", "z", true, "c", 2L, 24L, "owner", false);

            await store.InsertPartitionAsync(partitionToSplit, CancellationToken.None);

            Partition partitionToUpdate = partitionToSplit with
            {
                Last = "m",
                Remaining = 10L
            };

            Guid partitionToInsertId = Guid.NewGuid();
            DateTime partitionToInsertCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionToInsertUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partitionToInsert = new Partition(partitionToInsertId, jobId, partitionToInsertCreated, partitionToInsertUpdated, "n", "z", true, null, 0L, 12L, null, false);

            await store.InsertSplitPartitionAsync(partitionToUpdate, partitionToInsert, CancellationToken.None);

            Partition updated = await store.RetrievePartitionAsync(partitionToSplitId, CancellationToken.None);
            Partition inserted = await store.RetrievePartitionAsync(partitionToInsertId, CancellationToken.None);

            Assert.That(updated, Is.EqualTo(partitionToUpdate));
            Assert.That(inserted, Is.EqualTo(partitionToInsert));
        }

        [Test]
        public async Task CountIncompletePartitionsAsync_ArgumentJobIdIsNull_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<ArgumentNullException>(() => store.CountIncompletePartitionsAsync(null, CancellationToken.None));
        }

        [Test]
        public async Task CountIncompletePartitionsAsync_JobDoesNotExist_ThrowsException()
        {
            TStore store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.CountIncompletePartitionsAsync("DPJ-0001", CancellationToken.None));
        }

        [Test]
        public async Task CountIncompletePartitionsAsync_ContainsInclompletePartitions_ReturnsCount()
        {
            TStore store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            DateTime jobCreated = new DateTime(2022, 9, 12, 16, 21, 48, DateTimeKind.Utc);
            Job job = new Job(jobId, jobCreated, jobCreated, JobState.Processing);

            await store.InsertJobAsync(job, CancellationToken.None);

            Guid completedId = Guid.NewGuid();
            DateTime completedCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime completedUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition completed = new Partition(completedId, jobId, completedCreated, completedUpdated, "a", "m", false, "l", 13L, 0L, "owner1", true, false);

            await store.InsertPartitionAsync(completed, CancellationToken.None);

            Guid pendingId = Guid.NewGuid();
            DateTime pendingCreated = new DateTime(2022, 9, 12, 16, 23, 12, DateTimeKind.Utc);
            DateTime pendingUpdated = new DateTime(2022, 9, 12, 16, 24, 32, DateTimeKind.Utc);

            Partition pending = new Partition(pendingId, jobId, pendingCreated, pendingUpdated, "n", "z", true, "x", 8L, 2L, "owner2", false, false);

            await store.InsertPartitionAsync(pending, CancellationToken.None);

            long count = await store.CountIncompletePartitionsAsync(jobId, CancellationToken.None);

            Assert.That(count, Is.EqualTo(1L));
        }
    }
}
