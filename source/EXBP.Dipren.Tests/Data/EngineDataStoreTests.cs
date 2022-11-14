
using System.Collections;
using System.Diagnostics;

using EXBP.Dipren.Data;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Data
{
    public abstract class EngineDataStoreTests
    {
        protected abstract Task<IEngineDataStore> OnCreateEngineDataStoreAsync();

        protected virtual DateTime FormatDateTime(DateTime source)
            => source;

        protected virtual DateTime? FormatDateTime(DateTime? source)
            => (source != null) ? this.FormatDateTime(source.Value) : source;

        private async Task<EngineDataStoreWrapper> CreateEngineDataStoreAsync()
        {
            IEngineDataStore store = await this.OnCreateEngineDataStoreAsync();
            EngineDataStoreWrapper result = new EngineDataStoreWrapper(store);

            return result;
        }


        [Test]
        public async Task CountJobsAsync_NoJobsAvailable_RetrunsZero()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            long result = await store.CountJobsAsync(CancellationToken.None);

            Assert.That(result, Is.Zero);
        }

        [Test]
        public async Task CountJobsAsync_MultipleJobsAvailable_RetrunsCorrectCount()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            DateTime created = this.FormatDateTime(new DateTime(2022, 9, 21, 11, 12, 13, DateTimeKind.Utc));
            DateTime started = this.FormatDateTime(new DateTime(2022, 9, 21, 11, 16, 27, DateTimeKind.Utc));
            DateTime completed = this.FormatDateTime(new DateTime(2022, 9, 23, 17, 48, 48, DateTimeKind.Utc));

            Job job1 = new Job("DPJ-0001", created, created, JobState.Initializing, null, null);
            Job job2 = new Job("DPJ-0002", created, created, JobState.Ready, null, null);
            Job job3 = new Job("DPJ-0003", created, started, JobState.Processing, started, null);
            Job job4 = new Job("DPJ-0004", created, completed, JobState.Completed, started, completed);
            Job job5 = new Job("DPJ-0005", created, started, JobState.Failed);

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
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = null;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.InsertJobAsync(job, CancellationToken.None));
        }

        [TestCaseSource(nameof(InsertJobAsync_ArgumentJobIsValid_ParameterSource))]
        public async Task InsertJobAsync_ArgumentJobIsValid_InsertsJob(string id, DateTime created, DateTime updated, JobState state, DateTime? started, DateTime? completed, string error)
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            created = this.FormatDateTime(created);
            updated = this.FormatDateTime(updated);
            started = this.FormatDateTime(started);
            completed = this.FormatDateTime(completed);

            Job job = new Job(id, created, updated, state, started, completed, error);

            await store.InsertJobAsync(job, CancellationToken.None);

            Job retrieved = await store.RetrieveJobAsync(id, CancellationToken.None);

            Assert.That(retrieved, Is.EqualTo(job));
        }

        public static IEnumerable InsertJobAsync_ArgumentJobIsValid_ParameterSource()
        {
            DateTime created = new DateTime(2022, 9, 21, 11, 12, 13, DateTimeKind.Utc);
            DateTime started = new DateTime(2022, 9, 21, 11, 16, 27, DateTimeKind.Utc);
            DateTime completed = new DateTime(2022, 9, 23, 17, 48, 48, DateTimeKind.Utc);

            yield return new object[] { "DPJ-0001", created, created, JobState.Initializing, null, null, null };
            yield return new object[] { "DPJ-0002", created, created, JobState.Ready, null, null, null };
            yield return new object[] { "DPJ-0003", created, started, JobState.Processing, started, null, null };
            yield return new object[] { "DPJ-0004", created, completed, JobState.Completed, started, completed, null };
            yield return new object[] { "DPJ-0005", created, started, JobState.Failed, null, null, "error description" };
        }


        [Test]
        public async Task InsertJobAsync_JobWithSameIdentifierAlreadyExists_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            const string id = "DPJ-0001";
            DateTime timestamp = this.FormatDateTime(DateTime.UtcNow);

            Job first = new Job(id, timestamp, timestamp, JobState.Initializing);

            await store.InsertJobAsync(first, CancellationToken.None);

            Job second = first with { };

            Assert.ThrowsAsync<DuplicateIdentifierException>(() => store.InsertJobAsync(second, CancellationToken.None));
        }

        [Test]
        public async Task UpdateJobAsync_ArgumentJoIdIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<ArgumentNullException>(() => store.UpdateJobAsync(null, DateTime.UtcNow, JobState.Completed, null, CancellationToken.None));
        }

        [Test]
        public async Task UpdateJobAsync_JobDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.UpdateJobAsync("DPJ-0001", DateTime.UtcNow, JobState.Completed, null, CancellationToken.None));
        }

        [Test]
        public async Task UpdateJobAsync_JobExists_JobIsUpdated()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

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
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

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
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<ArgumentNullException>(() => store.RetrieveJobAsync(null, CancellationToken.None));
        }

        [Test]
        public async Task RetirveJobAsync_JobDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.RetrieveJobAsync("DPJ-0001", CancellationToken.None));
        }

        [TestCaseSource(nameof(RetirveJobAsync_ArgumentIdIsValidAndJobExists_ParameterSource))]
        public async Task RetirveJobAsync_ArgumentIdIsValidAndJobExists_RetrievesJob(string id, DateTime created, DateTime updated, JobState state, DateTime? started, DateTime? completed, string error)
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            created = this.FormatDateTime(created);
            updated = this.FormatDateTime(updated);
            started = this.FormatDateTime(started);
            completed = this.FormatDateTime(completed);

            Job job = new Job(id, created, updated, state, started, completed, error);

            await store.InsertJobAsync(job, CancellationToken.None);

            Job retrieved = await store.RetrieveJobAsync(id, CancellationToken.None);

            Assert.That(retrieved, Is.EqualTo(job));
        }

        public static IEnumerable RetirveJobAsync_ArgumentIdIsValidAndJobExists_ParameterSource()
        {
            DateTime created = new DateTime(2022, 9, 21, 11, 12, 13, DateTimeKind.Utc);
            DateTime started = new DateTime(2022, 9, 21, 11, 16, 27, DateTimeKind.Utc);
            DateTime completed = new DateTime(2022, 9, 23, 17, 48, 48, DateTimeKind.Utc);

            yield return new object[] { "DPJ-0001", created, created, JobState.Initializing, null, null, null };
            yield return new object[] { "DPJ-0002", created, created, JobState.Ready, null, null, null };
            yield return new object[] { "DPJ-0003", created, started, JobState.Processing, started, null, null };
            yield return new object[] { "DPJ-0004", created, completed, JobState.Completed, started, completed, null };
            yield return new object[] { "DPJ-0005", created, started, JobState.Failed, null, null, "error description" };
        }

        [Test]
        public async Task InsertPartitionAsync_ArgumentPartitionIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();
            Partition partition = null;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.InsertPartitionAsync(partition, CancellationToken.None));
        }

        [Test]
        public async Task InsertPartitionAsync_ReferencedJobDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            const string jobId = "DPJ-0001";
            Guid partitionId = Guid.NewGuid();
            DateTime timestamp = this.FormatDateTime(DateTime.UtcNow);

            Partition partition = new Partition(partitionId, jobId, timestamp, timestamp, "a", "z", true, "g", 7L, 18L);

            Assert.ThrowsAsync<InvalidReferenceException>(() => store.InsertPartitionAsync(partition, CancellationToken.None));
        }

        [Test]
        public async Task InsertPartitionAsync_PartitionWithSameIdentifierAlreadyExists_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid partitionId = Guid.NewGuid();
            DateTime created = this.FormatDateTime(new DateTime(2022, 9, 12, 11, 28, 44, DateTimeKind.Utc));
            DateTime updated = this.FormatDateTime(new DateTime(2022, 9, 12, 18, 33, 54, DateTimeKind.Utc));
            Partition first = new Partition(partitionId, job.Id, created, updated, "a", "z", true, "g", 7L, 18L);

            await store.InsertPartitionAsync(first, CancellationToken.None);

            Partition second = first with { };

            Assert.ThrowsAsync<DuplicateIdentifierException>(() => store.InsertPartitionAsync(second, CancellationToken.None));
        }

        [Test]
        public async Task InsertPartitionAsync_ArgumentPartitionIsValid_InsertsPartition()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = this.FormatDateTime(new DateTime(2022, 9, 12, 11, 28, 44, DateTimeKind.Utc));
            DateTime updated = this.FormatDateTime(new DateTime(2022, 9, 12, 18, 33, 54, DateTimeKind.Utc));
            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "g", 7L, 18L);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            Partition retrieved = await store.RetrievePartitionAsync(id, CancellationToken.None);

            Assert.That(retrieved, Is.EqualTo(partition));
        }

        [Test]
        public async Task RetirvePartitionAsync_PartitionDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Guid id = Guid.NewGuid();

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.RetrievePartitionAsync(id, CancellationToken.None));
        }

        [Test]
        public async Task RetirvePartitionAsync_PartitionExists_ReturnsPartition()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = this.FormatDateTime(new DateTime(2022, 9, 12, 11, 28, 44, DateTimeKind.Utc));
            DateTime updated = this.FormatDateTime(new DateTime(2022, 9, 12, 18, 33, 54, DateTimeKind.Utc));
            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "owner", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            Partition persisted = await store.RetrievePartitionAsync(id, CancellationToken.None);

            Assert.That(persisted, Is.EqualTo(partition));
        }

        [Test]
        public async Task TryAcquirePartitionAsync_ArgumentJobIdIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            DateTime now = DateTime.UtcNow;
            DateTime cut = now.AddMinutes(-2);

            Assert.ThrowsAsync<ArgumentNullException>(() => store.TryAcquirePartitionAsync(null, "owner", now, cut, CancellationToken.None));
        }

        [Test]
        public async Task TryAcquirePartitionAsync_ArgumentRequesterIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            DateTime now = DateTime.UtcNow;
            DateTime cut = now.AddMinutes(-2);

            Assert.ThrowsAsync<ArgumentNullException>(() => store.TryAcquirePartitionAsync("DPJ-0001", null, now, cut, CancellationToken.None));
        }

        [Test]
        public async Task TryAcquirePartitionAsync_SpecifiedJobDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            DateTime now = DateTime.UtcNow;
            DateTime cut = now.AddMinutes(-2);

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.TryAcquirePartitionAsync("DPJ-0001", "owner", now, cut, CancellationToken.None));
        }

        [Test]
        public async Task TryAcquirePartitionAsync_NoPartitionsExist_ReturnsNull()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            DateTime now = this.FormatDateTime(new DateTime(2022, 9, 13, 14, 11, 52, DateTimeKind.Utc));
            DateTime cut = this.FormatDateTime(new DateTime(2022, 9, 13, 14, 11, 50, DateTimeKind.Utc));

            Partition partition = await store.TryAcquirePartitionAsync(job.Id, "owner", now, cut, CancellationToken.None);

            Assert.That(partition, Is.Null);
        }

        [Test]
        public async Task TryAcquirePartitionAsync_OnlyActivePartitionsExist_ReturnsNull()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 13, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 13, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "b", 1L, 23L, "owner", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime now = new DateTime(2022, 9, 13, 16, 40, 0, DateTimeKind.Utc);
            DateTime cut = new DateTime(2022, 9, 13, 16, 23, 30, DateTimeKind.Utc);

            Partition acquired = await store.TryAcquirePartitionAsync(job.Id, "owner", now, cut, CancellationToken.None);

            Assert.That(acquired, Is.Null);
        }

        [Test]
        public async Task TryAcquirePartitionAsync_OnlyCompletedPartitionsExist_ReturnsNull()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "z", 24L, 0L, null, true, false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime now = new DateTime(2022, 9, 12, 16, 40, 0, DateTimeKind.Utc);
            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 32, DateTimeKind.Utc);

            Partition acquired = await store.TryAcquirePartitionAsync(job.Id, "owner", now, cut, CancellationToken.None);

            Assert.That(acquired, Is.Null);
        }

        [Test]
        public async Task TryAcquirePartitionAsync_FreePartitionsExist_ReturnsPartition()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "b", 1L, 23L, null, false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime now = new DateTime(2022, 9, 12, 16, 40, 0, DateTimeKind.Utc);
            DateTime cut = new DateTime(2022, 9, 12, 16, 22, 10, DateTimeKind.Utc);

            Partition acquired = await store.TryAcquirePartitionAsync(job.Id, "owner", now, cut, CancellationToken.None);

            Assert.That(acquired, Is.Not.Null);
            Assert.That(acquired.Owner, Is.EqualTo("owner"));
            Assert.That(acquired.Updated, Is.EqualTo(now));
        }

        [Test]
        public async Task TryAcquirePartitionAsync_FreePartitionsExist_OnlyOneIsUpdated()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id1 = Guid.NewGuid();
            Guid id2 = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);

            Partition partition1 = new Partition(id1, job.Id, created, updated, "a", "c", true, "b", 1L, 1L, null, false);
            Partition partition2 = new Partition(id2, job.Id, created, updated, "d", "f", true, "e", 1L, 1L, null, false);

            await store.InsertPartitionAsync(partition1, CancellationToken.None);
            await store.InsertPartitionAsync(partition2, CancellationToken.None);

            DateTime now = new DateTime(2022, 9, 12, 16, 40, 0, DateTimeKind.Utc);
            DateTime cut = new DateTime(2022, 9, 12, 16, 22, 10, DateTimeKind.Utc);

            Partition acquired = await store.TryAcquirePartitionAsync(job.Id, "owner", now, cut, CancellationToken.None);

            Assert.That(acquired, Is.Not.Null);
            Assert.That(acquired.Owner, Is.EqualTo("owner"));
            Assert.That(acquired.Updated, Is.EqualTo(now));

            Partition partition = await store.RetrievePartitionAsync((acquired.Id == id1) ? id2 : id1, CancellationToken.None);

            Assert.That(partition.Owner, Is.Null);
        }

        [Test]
        public async Task TryAcquirePartitionAsync_AbandonedPartitionsExist_ReturnsPartition()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "b", 1L, 23L, "other", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime now = new DateTime(2022, 9, 12, 16, 40, 0, DateTimeKind.Utc);
            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 32, DateTimeKind.Utc);

            Partition acquired = await store.TryAcquirePartitionAsync(job.Id, "owner", now, cut, CancellationToken.None);

            Assert.That(acquired, Is.Not.Null);
            Assert.That(acquired.Owner, Is.EqualTo("owner"));
            Assert.That(acquired.Updated, Is.EqualTo(now));
        }

        [Test]
        public async Task TryRequestSplitAsync_ArgumentJobIdIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            DateTime cut = DateTime.UtcNow.AddMinutes(-2);

            Assert.ThrowsAsync<ArgumentNullException>(() => store.TryRequestSplitAsync(null, cut, CancellationToken.None));
        }

        [Test]
        public async Task TryRequestSplitAsync_SpecifiedJobDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            DateTime now = new DateTime(2022, 9, 12, 16, 40, 0, DateTimeKind.Utc);
            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 32, DateTimeKind.Utc);

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.TryRequestSplitAsync("DPJ-0001", cut, CancellationToken.None));
        }

        [Test]
        public async Task TryRequestSplitAsync_NoPartitionsExist_ReturnsFalse()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            DateTime now = new DateTime(2022, 9, 12, 16, 40, 0, DateTimeKind.Utc);
            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 32, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(job.Id, cut, CancellationToken.None);

            Assert.That(result, Is.False);
        }

        [Test]
        public async Task TryRequestSplitAsync_OnlyFreePartitionsExist_ReturnsFalse()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "a", 0L, 24L, null, false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 30, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(job.Id, cut, CancellationToken.None);

            Assert.That(result, Is.False);
        }

        [Test]
        public async Task TryRequestSplitAsync_OnlyAbandonedPartitionsExist_ReturnsFalse()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 30, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "b", 1L, 23L, "owner", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(job.Id, cut, CancellationToken.None);

            Assert.That(result, Is.False);
        }

        [Test]
        public async Task TryRequestSplitAsync_OnlyCompletedPartitionsExist_ReturnsFalse()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "z", 24L, 0L, "owner", true);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 30, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(job.Id, cut, CancellationToken.None);

            Assert.That(result, Is.False);
        }

        [Test]
        public async Task TryRequestSplitAsync_OnlyPartitionsWithSplitRequestExist_ReturnsFalse()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "owner", false, true);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 30, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(job.Id, cut, CancellationToken.None);

            Assert.That(result, Is.False);
        }

        [Test]
        public async Task TryRequestSplitAsync_ActivePartitionsExist_ReturnsTrue()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "owner", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 30, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(job.Id, cut, CancellationToken.None);

            Assert.That(result, Is.True);

            Partition persisted = await store.RetrievePartitionAsync(id, CancellationToken.None);

            Assert.That(persisted.IsSplitRequested, Is.True);
        }

        [Test]
        public async Task ReportProgressAsync_ArgumentOwnerIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.ReportProgressAsync(id, null, timestamp, "d", 4, false, CancellationToken.None));
        }

        [Test]
        public async Task ReportProgressAsync_ArgumentPositionIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.ReportProgressAsync(id, "owner", timestamp, null, 4, false, CancellationToken.None));
        }

        [Test]
        public async Task ReportProgressAsync_SpecifiedPartitionDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.ReportProgressAsync(id, "owner", timestamp, "d", 4, false, CancellationToken.None));
        }

        [Test]
        public async Task ReportProgressAsync_PartitionLockNoLongerOwned_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "other", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime progressUpdated = new DateTime(2022, 9, 12, 16, 26, 11, DateTimeKind.Utc);

            Assert.ThrowsAsync<LockException>(() => store.ReportProgressAsync(id, "owner", progressUpdated, "g", 3, false, CancellationToken.None));
        }

        [Test]
        public async Task ReportProgressAsync_ValidArguments_UpdatesPartition()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "owner", false, false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime progressUpdated = new DateTime(2022, 9, 12, 16, 26, 11, DateTimeKind.Utc);

            await store.ReportProgressAsync(id, "owner", progressUpdated, "g", 3L, true, CancellationToken.None);

            Partition persisted = await store.RetrievePartitionAsync(id, CancellationToken.None);

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
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "owner", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime progressUpdated = new DateTime(2022, 9, 12, 16, 26, 11, DateTimeKind.Utc);

            Partition returned = await store.ReportProgressAsync(id, "owner", progressUpdated, "g", 3L, true, CancellationToken.None);

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
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "owner", false);

            Assert.ThrowsAsync<ArgumentNullException>(() => store.InsertSplitPartitionAsync(null, partition, CancellationToken.None));
        }

        [Test]
        public async Task InsertSplitPartition_ArgumentPartitionToInsertIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "g", 7L, 18L, "owner", false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            Assert.ThrowsAsync<ArgumentNullException>(() => store.InsertSplitPartitionAsync(partition, null, CancellationToken.None));
        }

        [Test]
        public async Task InsertSplitPartition_PartitionToUpdateDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid partitionToUpdateId = Guid.NewGuid();
            DateTime partitionToUpdateCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionToUpdateUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partitionToUpdate = new Partition(partitionToUpdateId, job.Id, partitionToUpdateCreated, partitionToUpdateUpdated, "a", "m", true, "c", 2L, 12L, "owner", false);

            Guid partitionToInsertId = Guid.NewGuid();
            DateTime partitionToInsertCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionToInsertUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partitionToInsert = new Partition(partitionToInsertId, job.Id, partitionToInsertCreated, partitionToInsertUpdated, "n", "z", true, null, 0L, 12L, null, false);

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.InsertSplitPartitionAsync(partitionToUpdate, partitionToInsert, CancellationToken.None));
        }

        [Test]
        public async Task InsertSplitPartition_PartitionToInsertAlreadyExists_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid partitionToUpdateId = Guid.NewGuid();
            DateTime partitionToUpdateCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionToUpdateUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partitionToUpdate = new Partition(partitionToUpdateId, job.Id, partitionToUpdateCreated, partitionToUpdateUpdated, "a", "m", true, "c", 2L, 12L, "owner", false);

            await store.InsertPartitionAsync(partitionToUpdate, CancellationToken.None);

            Guid partitionToInsertId = Guid.NewGuid();
            DateTime partitionToInsertCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionToInsertUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partitionToInsert = new Partition(partitionToInsertId, job.Id, partitionToInsertCreated, partitionToInsertUpdated, "n", "z", true, null, 0L, 12L, null, false);

            await store.InsertPartitionAsync(partitionToInsert, CancellationToken.None);

            Assert.ThrowsAsync<DuplicateIdentifierException>(() => store.InsertSplitPartitionAsync(partitionToUpdate, partitionToInsert, CancellationToken.None));
        }

        [Test]
        public async Task InsertSplitPartition_ArgumensAreValid_UpdatesExistingAndInsertsNewPartition()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid partitionToSplitId = Guid.NewGuid();
            DateTime partitionToSplitCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionToSplitUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partitionToSplit = new Partition(partitionToSplitId, job.Id, partitionToSplitCreated, partitionToSplitUpdated, "a", "z", true, "c", 2L, 24L, "owner", false);

            await store.InsertPartitionAsync(partitionToSplit, CancellationToken.None);

            Partition partitionToUpdate = partitionToSplit with
            {
                Last = "m",
                Remaining = 10L
            };

            Guid partitionToInsertId = Guid.NewGuid();
            DateTime partitionToInsertCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime partitionToInsertUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partitionToInsert = new Partition(partitionToInsertId, job.Id, partitionToInsertCreated, partitionToInsertUpdated, "n", "z", true, null, 0L, 12L, null, false);

            await store.InsertSplitPartitionAsync(partitionToUpdate, partitionToInsert, CancellationToken.None);

            Partition updated = await store.RetrievePartitionAsync(partitionToSplitId, CancellationToken.None);
            Partition inserted = await store.RetrievePartitionAsync(partitionToInsertId, CancellationToken.None);

            Assert.That(updated, Is.EqualTo(partitionToUpdate));
            Assert.That(inserted, Is.EqualTo(partitionToInsert));
        }

        [Test]
        public async Task CountIncompletePartitionsAsync_ArgumentJobIdIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<ArgumentNullException>(() => store.CountIncompletePartitionsAsync(null, CancellationToken.None));
        }

        [Test]
        public async Task CountIncompletePartitionsAsync_JobDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.CountIncompletePartitionsAsync("DPJ-0001", CancellationToken.None));
        }

        [Test]
        public async Task CountIncompletePartitionsAsync_ContainsInclompletePartitions_ReturnsCount()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid completedId = Guid.NewGuid();
            DateTime completedCreated = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime completedUpdated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition completed = new Partition(completedId, job.Id, completedCreated, completedUpdated, "a", "m", false, "l", 13L, 0L, "owner1", true, false);

            await store.InsertPartitionAsync(completed, CancellationToken.None);

            Guid pendingId = Guid.NewGuid();
            DateTime pendingCreated = new DateTime(2022, 9, 12, 16, 23, 12, DateTimeKind.Utc);
            DateTime pendingUpdated = new DateTime(2022, 9, 12, 16, 24, 32, DateTimeKind.Utc);

            Partition pending = new Partition(pendingId, job.Id, pendingCreated, pendingUpdated, "n", "z", true, "x", 8L, 2L, "owner2", false, false);

            await store.InsertPartitionAsync(pending, CancellationToken.None);

            long count = await store.CountIncompletePartitionsAsync(job.Id, CancellationToken.None);

            Assert.That(count, Is.EqualTo(1L));
        }

        private async Task<Job> EnsurePersistedJobAsync(IEngineDataStore store, JobState state, string id = "DPJ-0001", CancellationToken cancellation = default)
        {
            DateTime created = this.FormatDateTime(new DateTime(2022, 9, 07, 16, 21, 48, DateTimeKind.Utc));
            DateTime updated = created;
            DateTime? started = null;
            DateTime? completed = null;
            string error = null;

            switch (state)
            {
                case JobState.Initializing:
                case JobState.Ready:
                    break;

                case JobState.Failed:
                    error = "Could not schedule job due to some error.";
                    break;

                case JobState.Processing:
                    started = this.FormatDateTime(new DateTime(2022, 9, 12, 11, 27, 28, DateTimeKind.Utc));
                    updated = started.Value;
                    break;

                case JobState.Completed:
                    started = this.FormatDateTime(new DateTime(2022, 9, 12, 11, 27, 28, DateTimeKind.Utc));
                    completed = this.FormatDateTime(new DateTime(2022, 9, 25, 22, 23, 56, DateTimeKind.Utc));
                    updated = completed.Value;
                    break;
            }

            Job result = new Job(id, created, updated, state, started, completed, error);

            await store.InsertJobAsync(result, cancellation);

            return result;
        }


        [DebuggerStepThrough]
        private class EngineDataStoreWrapper : IEngineDataStore, IDisposable
        {
            private readonly IEngineDataStore _store;

            public EngineDataStoreWrapper(IEngineDataStore store)
            {
                Debug.Assert(store != null);

                this._store = store;
            }

            public void Dispose()
            {
                (this._store as IDisposable)?.Dispose();

                GC.SuppressFinalize(this);
            }

            public Task<long> CountIncompletePartitionsAsync(string id, CancellationToken cancellation)
                => this._store.CountIncompletePartitionsAsync(id, cancellation);

            public Task<long> CountJobsAsync(CancellationToken cancellation)
                => this._store.CountJobsAsync(cancellation);

            public Task InsertJobAsync(Job job, CancellationToken cancellation)
                => this._store.InsertJobAsync(job, cancellation);

            public Task InsertPartitionAsync(Partition partition, CancellationToken cancellation)
                => this._store.InsertPartitionAsync(partition, cancellation);

            public Task InsertSplitPartitionAsync(Partition partitionToUpdate, Partition partitionToInsert, CancellationToken cancellation)
                => this._store.InsertSplitPartitionAsync(partitionToUpdate, partitionToInsert, cancellation);

            public Task<Partition> ReportProgressAsync(Guid id, string owner, DateTime timestamp, string position, long progress, bool completed, CancellationToken cancellation)
                => this._store.ReportProgressAsync(id, owner, timestamp, position, progress, completed, cancellation);

            public Task<Job> RetrieveJobAsync(string id, CancellationToken cancellation)
                => this._store.RetrieveJobAsync(id, cancellation);

            public Task<Partition> RetrievePartitionAsync(Guid id, CancellationToken cancellation)
                => this._store.RetrievePartitionAsync(id, cancellation);

            public Task<Partition> TryAcquirePartitionAsync(string jobId, string requester, DateTime timestamp, DateTime active, CancellationToken cancellation)
                => this._store.TryAcquirePartitionAsync(jobId, requester, timestamp, active, cancellation);

            public Task<bool> TryRequestSplitAsync(string jobId, DateTime active, CancellationToken cancellation)
                => this._store.TryRequestSplitAsync(jobId, active, cancellation);

            public Task<Job> UpdateJobAsync(string jobId, DateTime timestamp, JobState state, string error, CancellationToken cancellation)
                => this._store.UpdateJobAsync(jobId, timestamp, state, error, cancellation);

            public Task<Job> MarkJobAsReadyAsync(string id, DateTime timestamp, CancellationToken cancellation)
                => this._store.MarkJobAsReadyAsync(id, timestamp, cancellation);

            public Task<Job> MarkJobAsStartedAsync(string id, DateTime timestamp, CancellationToken cancellation)
                => this._store.MarkJobAsStartedAsync(id, timestamp, cancellation);

            public Task<Job> MarkJobAsCompletedAsync(string id, DateTime timestamp, CancellationToken cancellation)
                => this._store.MarkJobAsCompletedAsync(id, timestamp, cancellation);

            public Task<Job> MarkJobAsFailedAsync(string id, DateTime timestamp, string error, CancellationToken cancellation)
                => this._store.MarkJobAsFailedAsync(id, timestamp, error, cancellation);
        }
    }
}
