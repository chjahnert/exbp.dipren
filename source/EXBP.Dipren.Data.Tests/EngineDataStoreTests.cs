
using System.Collections;
using System.Diagnostics;

using NUnit.Framework;


namespace EXBP.Dipren.Data.Tests
{
    public abstract class EngineDataStoreTests
    {
        private readonly int DefaultBachSize = 66;
        private readonly TimeSpan DefaultTimeout = TimeSpan.FromMilliseconds(10200);
        private readonly TimeSpan DefaultClockDrift = TimeSpan.FromMilliseconds(2465);

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

            Job job1 = new Job("DPJ-0001", created, created, JobState.Initializing, this.DefaultBachSize, this.DefaultTimeout, this.DefaultClockDrift, null, null);
            Job job2 = new Job("DPJ-0002", created, created, JobState.Ready, this.DefaultBachSize, this.DefaultTimeout, this.DefaultClockDrift, null, null);
            Job job3 = new Job("DPJ-0003", created, started, JobState.Processing, this.DefaultBachSize, this.DefaultTimeout, this.DefaultClockDrift, started, null);
            Job job4 = new Job("DPJ-0004", created, completed, JobState.Completed, this.DefaultBachSize, this.DefaultTimeout, this.DefaultClockDrift, started, completed);
            Job job5 = new Job("DPJ-0005", created, started, JobState.Failed, this.DefaultBachSize, this.DefaultTimeout, this.DefaultClockDrift);

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

            Job job = new Job(id, created, updated, state, this.DefaultBachSize, this.DefaultTimeout, this.DefaultClockDrift, started, completed, error);

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

            Job first = new Job(id, timestamp, timestamp, JobState.Initializing, this.DefaultBachSize, this.DefaultTimeout, this.DefaultClockDrift);

            await store.InsertJobAsync(first, CancellationToken.None);

            Job second = first with { };

            Assert.ThrowsAsync<DuplicateIdentifierException>(() => store.InsertJobAsync(second, CancellationToken.None));
        }

        [Test]
        public async Task MarkJobAsReadyAsync_ArgumentIdIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<ArgumentNullException>(() => store.MarkJobAsReadyAsync(null, DateTime.UtcNow, CancellationToken.None));
        }

        [Test]
        public async Task MarkJobAsReadyAsync_JobDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.MarkJobAsReadyAsync("DPJ-0001", DateTime.UtcNow, CancellationToken.None));
        }

        [Test]
        public async Task MarkJobAsReadyAsync_JobExists_UpdatesJob()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job persisted = await this.EnsurePersistedJobAsync(store, JobState.Initializing);

            DateTime timestamp = this.FormatDateTime(new DateTime(2022, 9, 11, 11, 6, 1, DateTimeKind.Utc));

            await store.MarkJobAsReadyAsync(persisted.Id, timestamp, CancellationToken.None);

            Job retrieved = await store.RetrieveJobAsync(persisted.Id, CancellationToken.None);

            Assert.That(retrieved, Is.Not.Null);
            Assert.That(retrieved.Created, Is.EqualTo(persisted.Created));
            Assert.That(retrieved.Updated, Is.EqualTo(timestamp));
            Assert.That(retrieved.State, Is.EqualTo(JobState.Ready));
            Assert.That(retrieved.Started, Is.Null);
            Assert.That(retrieved.Completed, Is.Null);
            Assert.That(retrieved.Error, Is.Null);
        }

        [Test]
        public async Task MarkJobAsReadyAsync_JobExists_ReturnsUpdatedJob()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job persisted = await this.EnsurePersistedJobAsync(store, JobState.Initializing);

            DateTime timestamp = this.FormatDateTime(new DateTime(2022, 9, 11, 11, 6, 1, DateTimeKind.Utc));

            Job updated = await store.MarkJobAsReadyAsync(persisted.Id, timestamp, CancellationToken.None);

            Assert.That(updated, Is.Not.Null);
            Assert.That(updated.Created, Is.EqualTo(persisted.Created));
            Assert.That(updated.Updated, Is.EqualTo(timestamp));
            Assert.That(updated.State, Is.EqualTo(JobState.Ready));
            Assert.That(updated.Started, Is.Null);
            Assert.That(updated.Completed, Is.Null);
            Assert.That(updated.Error, Is.Null);
        }

        [Test]
        public async Task MarkJobAsStartedAsync_ArgumentIdIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<ArgumentNullException>(() => store.MarkJobAsStartedAsync(null, DateTime.UtcNow, CancellationToken.None));
        }

        [Test]
        public async Task MarkJobAsStartedAsync_JobDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.MarkJobAsStartedAsync("DPJ-0001", DateTime.UtcNow, CancellationToken.None));
        }

        [Test]
        public async Task MarkJobAsStartedAsync_JobExists_UpdatesJob()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job persisted = await this.EnsurePersistedJobAsync(store, JobState.Ready);

            DateTime timestamp = this.FormatDateTime(new DateTime(2022, 9, 11, 11, 6, 1, DateTimeKind.Utc));

            await store.MarkJobAsStartedAsync(persisted.Id, timestamp, CancellationToken.None);

            Job retrieved = await store.RetrieveJobAsync(persisted.Id, CancellationToken.None);

            Assert.That(retrieved, Is.Not.Null);
            Assert.That(retrieved.Created, Is.EqualTo(persisted.Created));
            Assert.That(retrieved.Updated, Is.EqualTo(timestamp));
            Assert.That(retrieved.State, Is.EqualTo(JobState.Processing));
            Assert.That(retrieved.Started, Is.EqualTo(timestamp));
            Assert.That(retrieved.Completed, Is.Null);
            Assert.That(retrieved.Error, Is.Null);
        }

        [Test]
        public async Task MarkJobAsStartedAsync_JobExists_ReturnsUpdatedJob()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job persisted = await this.EnsurePersistedJobAsync(store, JobState.Ready);

            DateTime timestamp = this.FormatDateTime(new DateTime(2022, 9, 11, 11, 6, 1, DateTimeKind.Utc));

            Job updated = await store.MarkJobAsStartedAsync(persisted.Id, timestamp, CancellationToken.None);

            Assert.That(updated, Is.Not.Null);
            Assert.That(updated.Created, Is.EqualTo(persisted.Created));
            Assert.That(updated.Updated, Is.EqualTo(timestamp));
            Assert.That(updated.State, Is.EqualTo(JobState.Processing));
            Assert.That(updated.Started, Is.EqualTo(timestamp));
            Assert.That(updated.Completed, Is.Null);
            Assert.That(updated.Error, Is.Null);
        }

        [Test]
        public async Task MarkJobAsCompletedAsync_ArgumentIdIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<ArgumentNullException>(() => store.MarkJobAsCompletedAsync(null, DateTime.UtcNow, CancellationToken.None));
        }

        [Test]
        public async Task MarkJobAsCompletedAsync_JobDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.MarkJobAsCompletedAsync("DPJ-0001", DateTime.UtcNow, CancellationToken.None));
        }

        [Test]
        public async Task MarkJobAsCompletedAsync_JobExists_UpdatesJob()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job persisted = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            DateTime timestamp = this.FormatDateTime(new DateTime(2022, 9, 11, 11, 6, 1, DateTimeKind.Utc));

            await store.MarkJobAsCompletedAsync(persisted.Id, timestamp, CancellationToken.None);

            Job retrieved = await store.RetrieveJobAsync(persisted.Id, CancellationToken.None);

            Assert.That(retrieved, Is.Not.Null);
            Assert.That(retrieved.Created, Is.EqualTo(persisted.Created));
            Assert.That(retrieved.Updated, Is.EqualTo(timestamp));
            Assert.That(retrieved.State, Is.EqualTo(JobState.Completed));
            Assert.That(retrieved.Started, Is.EqualTo(persisted.Started));
            Assert.That(retrieved.Completed, Is.EqualTo(timestamp));
            Assert.That(retrieved.Error, Is.Null);
        }

        [Test]
        public async Task MarkJobAsCompletedAsync_JobExists_ReturnsUpdatedJob()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job persisted = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            DateTime timestamp = this.FormatDateTime(new DateTime(2022, 9, 11, 11, 6, 1, DateTimeKind.Utc));

            Job updated = await store.MarkJobAsCompletedAsync(persisted.Id, timestamp, CancellationToken.None);

            Assert.That(updated, Is.Not.Null);
            Assert.That(updated.Created, Is.EqualTo(persisted.Created));
            Assert.That(updated.Updated, Is.EqualTo(timestamp));
            Assert.That(updated.State, Is.EqualTo(JobState.Completed));
            Assert.That(updated.Started, Is.EqualTo(persisted.Started));
            Assert.That(updated.Completed, Is.EqualTo(timestamp));
            Assert.That(updated.Error, Is.Null);
        }

        [Test]
        public async Task MarkJobAsFailedAsync_ArgumentIdIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<ArgumentNullException>(() => store.MarkJobAsFailedAsync(null, DateTime.UtcNow, "The description of the error.", CancellationToken.None));
        }

        [Test]
        public async Task MarkJobAsFailedAsync_JobDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.MarkJobAsFailedAsync("DPJ-0001", DateTime.UtcNow, "The description of the error.", CancellationToken.None));
        }

        [Test]
        public async Task MarkJobAsFailedAsync_JobExists_UpdatesJob()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job persisted = await this.EnsurePersistedJobAsync(store, JobState.Initializing);

            DateTime timestamp = this.FormatDateTime(new DateTime(2022, 9, 11, 11, 6, 1, DateTimeKind.Utc));
            const string error = "The description of the error.";

            await store.MarkJobAsFailedAsync(persisted.Id, timestamp, error, CancellationToken.None);

            Job retrieved = await store.RetrieveJobAsync(persisted.Id, CancellationToken.None);

            Assert.That(retrieved, Is.Not.Null);
            Assert.That(retrieved.Created, Is.EqualTo(persisted.Created));
            Assert.That(retrieved.Updated, Is.EqualTo(timestamp));
            Assert.That(retrieved.State, Is.EqualTo(JobState.Failed));
            Assert.That(retrieved.Started, Is.Null);
            Assert.That(retrieved.Completed, Is.Null);
            Assert.That(retrieved.Error, Is.EqualTo(error));
        }

        [Test]
        public async Task MarkJobAsFailedAsync_JobExists_ReturnsUpdatedJob()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job persisted = await this.EnsurePersistedJobAsync(store, JobState.Initializing);

            DateTime timestamp = this.FormatDateTime(new DateTime(2022, 9, 11, 11, 6, 1, DateTimeKind.Utc));
            const string error = "The description of the error.";

            Job updated = await store.MarkJobAsFailedAsync(persisted.Id, timestamp, error, CancellationToken.None);

            Assert.That(updated, Is.Not.Null);
            Assert.That(updated.Created, Is.EqualTo(persisted.Created));
            Assert.That(updated.Updated, Is.EqualTo(timestamp));
            Assert.That(updated.State, Is.EqualTo(JobState.Failed));
            Assert.That(updated.Started, Is.Null);
            Assert.That(updated.Completed, Is.Null);
            Assert.That(updated.Error, Is.EqualTo(error));
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

            Job job = new Job(id, created, updated, state, this.DefaultBachSize, this.DefaultTimeout, this.DefaultClockDrift, started, completed, error);

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

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "z", 24L, 0L, null, true, 4231.1, null);

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

            Assert.ThrowsAsync<ArgumentNullException>(() => store.TryRequestSplitAsync(null, "current", cut, CancellationToken.None));
        }

        [Test]
        public async Task TryRequestSplitAsync_ArgumentRequesterIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            DateTime cut = DateTime.UtcNow.AddMinutes(-2);

            Assert.ThrowsAsync<ArgumentNullException>(() => store.TryRequestSplitAsync("DPJ-001", null, cut, CancellationToken.None));
        }

        [Test]
        public async Task TryRequestSplitAsync_SpecifiedJobDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            DateTime now = new DateTime(2022, 9, 12, 16, 40, 0, DateTimeKind.Utc);
            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 32, DateTimeKind.Utc);

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.TryRequestSplitAsync("DPJ-0001", "current", cut, CancellationToken.None));
        }

        [Test]
        public async Task TryRequestSplitAsync_NoPartitionsExist_ReturnsFalse()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            DateTime now = new DateTime(2022, 9, 12, 16, 40, 0, DateTimeKind.Utc);
            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 32, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(job.Id, "current", cut, CancellationToken.None);

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

            bool result = await store.TryRequestSplitAsync(job.Id, "current", cut, CancellationToken.None);

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

            bool result = await store.TryRequestSplitAsync(job.Id, "current", cut, CancellationToken.None);

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

            bool result = await store.TryRequestSplitAsync(job.Id, "current", cut, CancellationToken.None);

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

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "owner", false, 4231.1, "other");

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime cut = new DateTime(2022, 9, 12, 16, 23, 30, DateTimeKind.Utc);

            bool result = await store.TryRequestSplitAsync(job.Id, "current", cut, CancellationToken.None);

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

            bool result = await store.TryRequestSplitAsync(job.Id, "current", cut, CancellationToken.None);

            Assert.That(result, Is.True);

            Partition persisted = await store.RetrievePartitionAsync(id, CancellationToken.None);

            Assert.That(persisted.IsSplitRequested, Is.True);
            Assert.That(persisted.SplitRequester, Is.EqualTo("current"));
        }

        [Test]
        public async Task IsSplitRequestPendingAsync_ArgumentJobIdIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<ArgumentNullException>(() => store.IsSplitRequestPendingAsync(null, "node-1", CancellationToken.None));
        }

        [Test]
        public async Task IsSplitRequestPendingAsync_ArgumentRequesterIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<ArgumentNullException>(() => store.IsSplitRequestPendingAsync("DJP-001", null, CancellationToken.None));
        }

        [Test]
        public async Task IsSplitRequestPendingAsync_SplitRequestIsPending_ReturnsTrue()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "other", false, 72.4, "node-1");

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            bool result = await store.IsSplitRequestPendingAsync(job.Id, "node-1", CancellationToken.None);

            Assert.That(result, Is.True);
        }

        [Test]
        public async Task IsSplitRequestPendingAsync_NoSplitRequested_ReturnsFalse()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "other", false, 72.4, "node-2");

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            bool result = await store.IsSplitRequestPendingAsync(job.Id, "node-1", CancellationToken.None);

            Assert.That(result, Is.False);
        }

        [Test]
        public async Task ReportProgressAsync_ArgumentOwnerIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.ReportProgressAsync(id, null, timestamp, "d", 4, 2, false, 4231.1, CancellationToken.None));
        }

        [Test]
        public async Task ReportProgressAsync_ArgumentPositionIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Assert.ThrowsAsync<ArgumentNullException>(() => store.ReportProgressAsync(id, "owner", timestamp, null, 4, 2, false, 4231.1, CancellationToken.None));
        }

        [Test]
        public async Task ReportProgressAsync_SpecifiedPartitionDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Guid id = Guid.NewGuid();
            DateTime timestamp = DateTime.UtcNow;

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.ReportProgressAsync(id, "owner", timestamp, "d", 4, 2, false, 4231.1, CancellationToken.None));
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

            Assert.ThrowsAsync<LockException>(() => store.ReportProgressAsync(id, "owner", progressUpdated, "g", 3, 1, false, 4231.1, CancellationToken.None));
        }

        [Test]
        public async Task ReportProgressAsync_PartitionIsNotCompleted_UpdatesPartition()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "owner", false, 0.0, "other");

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime progressUpdated = new DateTime(2022, 9, 12, 16, 26, 11, DateTimeKind.Utc);

            await store.ReportProgressAsync(id, "owner", progressUpdated, "g", 5L, 19L, false, 4231.1, CancellationToken.None);

            Partition persisted = await store.RetrievePartitionAsync(id, CancellationToken.None);

            Assert.That(persisted.JobId, Is.EqualTo(partition.JobId));
            Assert.That(persisted.Created, Is.EqualTo(partition.Created));
            Assert.That(persisted.Updated, Is.EqualTo(progressUpdated));
            Assert.That(persisted.Owner, Is.EqualTo("owner"));
            Assert.That(persisted.First, Is.EqualTo(partition.First));
            Assert.That(persisted.Last, Is.EqualTo(partition.Last));
            Assert.That(persisted.IsInclusive, Is.EqualTo(partition.IsInclusive));
            Assert.That(persisted.Position, Is.EqualTo("g"));
            Assert.That(persisted.Processed, Is.EqualTo(5L));
            Assert.That(persisted.Remaining, Is.EqualTo(19L));
            Assert.That(persisted.IsCompleted, Is.False);
            Assert.That(persisted.Throughput, Is.EqualTo(4231.1));
            Assert.That(persisted.IsSplitRequested, Is.True);
            Assert.That(persisted.SplitRequester, Is.EqualTo(partition.SplitRequester));
        }

        [Test]
        public async Task ReportProgressAsync_PartitionIsNotCompleted_ReturnsUpdatedPartition()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "owner", false, 0.0, "other");

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime progressUpdated = new DateTime(2022, 9, 12, 16, 26, 11, DateTimeKind.Utc);

            Partition returned = await store.ReportProgressAsync(id, "owner", progressUpdated, "g", 5L, 19L, false, 4231.1, CancellationToken.None);

            Assert.That(returned.JobId, Is.EqualTo(partition.JobId));
            Assert.That(returned.Created, Is.EqualTo(partition.Created));
            Assert.That(returned.Updated, Is.EqualTo(progressUpdated));
            Assert.That(returned.Owner, Is.EqualTo("owner"));
            Assert.That(returned.First, Is.EqualTo(partition.First));
            Assert.That(returned.Last, Is.EqualTo(partition.Last));
            Assert.That(returned.IsInclusive, Is.EqualTo(partition.IsInclusive));
            Assert.That(returned.Position, Is.EqualTo("g"));
            Assert.That(returned.Processed, Is.EqualTo(5L));
            Assert.That(returned.Remaining, Is.EqualTo(19L));
            Assert.That(returned.IsCompleted, Is.False);
            Assert.That(returned.Throughput, Is.EqualTo(4231.1));
            Assert.That(returned.IsSplitRequested, Is.True);
            Assert.That(returned.SplitRequester, Is.EqualTo(partition.SplitRequester));
        }

        [Test]
        public async Task ReportProgressAsync_PartitionIsCompleted_UpdatesPartition()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "owner", false, 0.0, "other");

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime progressUpdated = new DateTime(2022, 9, 12, 16, 26, 11, DateTimeKind.Utc);

            await store.ReportProgressAsync(id, "owner", progressUpdated, "g", 5L, 19L, true, 4231.1, CancellationToken.None);

            Partition persisted = await store.RetrievePartitionAsync(id, CancellationToken.None);

            Assert.That(persisted.JobId, Is.EqualTo(partition.JobId));
            Assert.That(persisted.Created, Is.EqualTo(partition.Created));
            Assert.That(persisted.Updated, Is.EqualTo(progressUpdated));
            Assert.That(persisted.Owner, Is.EqualTo("owner"));
            Assert.That(persisted.First, Is.EqualTo(partition.First));
            Assert.That(persisted.Last, Is.EqualTo(partition.Last));
            Assert.That(persisted.IsInclusive, Is.EqualTo(partition.IsInclusive));
            Assert.That(persisted.Position, Is.EqualTo("g"));
            Assert.That(persisted.Processed, Is.EqualTo(5L));
            Assert.That(persisted.Remaining, Is.EqualTo(19L));
            Assert.That(persisted.IsCompleted, Is.True);
            Assert.That(persisted.Throughput, Is.EqualTo(4231.1));
            Assert.That(persisted.IsSplitRequested, Is.False);
            Assert.That(persisted.SplitRequester, Is.Null);
        }

        [Test]
        public async Task ReportProgressAsync_PartitionIsCompleted_ReturnsUpdatedPartition()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "owner", false, 0.0, "other");

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime progressUpdated = new DateTime(2022, 9, 12, 16, 26, 11, DateTimeKind.Utc);

            Partition returned = await store.ReportProgressAsync(id, "owner", progressUpdated, "g", 5L, 19L, true, 4231.1, CancellationToken.None);

            Assert.That(returned.JobId, Is.EqualTo(partition.JobId));
            Assert.That(returned.Created, Is.EqualTo(partition.Created));
            Assert.That(returned.Updated, Is.EqualTo(progressUpdated));
            Assert.That(returned.Owner, Is.EqualTo("owner"));
            Assert.That(returned.First, Is.EqualTo(partition.First));
            Assert.That(returned.Last, Is.EqualTo(partition.Last));
            Assert.That(returned.IsInclusive, Is.EqualTo(partition.IsInclusive));
            Assert.That(returned.Position, Is.EqualTo("g"));
            Assert.That(returned.Processed, Is.EqualTo(5L));
            Assert.That(returned.Remaining, Is.EqualTo(19L));
            Assert.That(returned.IsCompleted, Is.True);
            Assert.That(returned.Throughput, Is.EqualTo(4231.1));
            Assert.That(returned.IsSplitRequested, Is.False);
            Assert.That(returned.SplitRequester, Is.Null);
        }

        [Test]
        public async Task ReportProgressAsync_ValidArguments_ReturnsUpdatedPartition()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);

            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 2L, 22L, "owner", false, 0.0);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            DateTime progressUpdated = new DateTime(2022, 9, 12, 16, 26, 11, DateTimeKind.Utc);

            Partition returned = await store.ReportProgressAsync(id, "owner", progressUpdated, "g", 5L, 19L, true, 4231.1, CancellationToken.None);

            Assert.That(returned.JobId, Is.EqualTo(partition.JobId));
            Assert.That(returned.Created, Is.EqualTo(partition.Created));
            Assert.That(returned.Updated, Is.EqualTo(progressUpdated));
            Assert.That(returned.Owner, Is.EqualTo("owner"));
            Assert.That(returned.First, Is.EqualTo(partition.First));
            Assert.That(returned.Last, Is.EqualTo(partition.Last));
            Assert.That(returned.IsInclusive, Is.EqualTo(partition.IsInclusive));
            Assert.That(returned.Position, Is.EqualTo("g"));
            Assert.That(returned.Processed, Is.EqualTo(5L));
            Assert.That(returned.Remaining, Is.EqualTo(19L));
            Assert.That(returned.IsCompleted, Is.True);
            Assert.That(returned.Throughput, Is.EqualTo(4231.1));
            Assert.That(returned.IsSplitRequested, Is.EqualTo(partition.IsSplitRequested));
            Assert.That(returned.SplitRequester, Is.EqualTo(partition.SplitRequester));
        }

        [Test]
        public async Task InsertSplitPartitionAsync_ArgumentPartitionToUpdateIsNull_ThrowsException()
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
        public async Task InsertSplitPartitionAsync_ArgumentPartitionToInsertIsNull_ThrowsException()
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
        public async Task InsertSplitPartitionAsync_PartitionToUpdateDoesNotExist_ThrowsException()
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
        public async Task InsertSplitPartitionAsync_PartitionToInsertAlreadyExists_ThrowsException()
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
        public async Task InsertSplitPartitionAsync_ArgumensAreValid_UpdatesExistingAndInsertsNewPartition()
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

            Partition completed = new Partition(completedId, job.Id, completedCreated, completedUpdated, "a", "m", false, "l", 13L, 0L, "owner1", true, 5123.7, null);

            await store.InsertPartitionAsync(completed, CancellationToken.None);

            Guid pendingId = Guid.NewGuid();
            DateTime pendingCreated = new DateTime(2022, 9, 12, 16, 23, 12, DateTimeKind.Utc);
            DateTime pendingUpdated = new DateTime(2022, 9, 12, 16, 24, 32, DateTimeKind.Utc);

            Partition pending = new Partition(pendingId, job.Id, pendingCreated, pendingUpdated, "n", "z", true, "x", 8L, 2L, "owner2", false, 5123.7, null);

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

            Job result = new Job(id, created, updated, state, this.DefaultBachSize, this.DefaultTimeout, this.DefaultClockDrift, started, completed, error);

            await store.InsertJobAsync(result, cancellation);

            return result;
        }

        [Test]
        public async Task RetrieveJobStatusReportAsync_ArgumentIdIsNull_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<ArgumentNullException>(() => store.RetrieveJobStatusReportAsync(null, DateTime.UtcNow, CancellationToken.None));
        }

        [Test]
        public async Task RetrieveJobStatusReportAsync_JobDoesNotExist_ThrowsException()
        {
            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Assert.ThrowsAsync<UnknownIdentifierException>(() => store.RetrieveJobStatusReportAsync("DPJ-0001", DateTime.UtcNow, CancellationToken.None));
        }

        [Test]
        public async Task RetrieveJobStatusReportAsync_JobIsInitializing_ReturnsCorrectResult()
        {
            DateTime timestamp = new DateTime(2022, 11, 17, 16, 21, 21, DateTimeKind.Utc);

            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Initializing);

            StatusReport result = await store.RetrieveJobStatusReportAsync(job.Id, timestamp, CancellationToken.None);

            Assert.That(result, Is.Not.Null);
            Assert.That(result.Id, Is.EqualTo(job.Id));
            Assert.That(result.Timestamp, Is.EqualTo(timestamp));
            Assert.That(result.Created, Is.EqualTo(job.Created));
            Assert.That(result.Updated, Is.EqualTo(job.Updated));
            Assert.That(result.BatchSize, Is.EqualTo(job.BatchSize));
            Assert.That(result.Timeout, Is.EqualTo(job.Timeout));
            Assert.That(result.Started, Is.EqualTo(job.Started));
            Assert.That(result.Completed, Is.EqualTo(job.Completed));
            Assert.That(result.State, Is.EqualTo(job.State));
            Assert.That(result.Error, Is.EqualTo(job.Error));

            Assert.That(result.Partitions, Is.Not.Null);
            Assert.That(result.Partitions.Untouched, Is.EqualTo(0L));
            Assert.That(result.Partitions.InProgress, Is.EqualTo(0L));
            Assert.That(result.Partitions.Completed, Is.EqualTo(0L));
            Assert.That(result.Partitions.Total, Is.EqualTo(0L));

            Assert.That(result.Progress, Is.Not.Null);
            Assert.That(result.Progress.Remaining, Is.Null);
            Assert.That(result.Progress.Completed, Is.Null);
            Assert.That(result.Progress.Total, Is.Null);
            Assert.That(result.Progress.Ratio, Is.Null);

            Assert.That(result.OwnershipChanges, Is.Zero);
            Assert.That(result.PendingSplitRequests, Is.Zero);
        }

        [Test]
        public async Task RetrieveJobStatusReportAsync_JobIsReady_ReturnsCorrectResult()
        {
            DateTime timestamp = new DateTime(2022, 11, 17, 16, 21, 21, DateTimeKind.Utc);

            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Ready);

            Guid id = Guid.NewGuid();
            DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
            DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);
            Partition partition = new Partition(id, job.Id, created, updated, "a", "z", true, "c", 0L, 26L, null, false);

            await store.InsertPartitionAsync(partition, CancellationToken.None);

            StatusReport result = await store.RetrieveJobStatusReportAsync(job.Id, timestamp, CancellationToken.None);

            Assert.That(result, Is.Not.Null);
            Assert.That(result.Id, Is.EqualTo(job.Id));
            Assert.That(result.Timestamp, Is.EqualTo(timestamp));
            Assert.That(result.Created, Is.EqualTo(job.Created));
            Assert.That(result.Updated, Is.EqualTo(job.Updated));
            Assert.That(result.BatchSize, Is.EqualTo(job.BatchSize));
            Assert.That(result.Timeout, Is.EqualTo(job.Timeout));
            Assert.That(result.Started, Is.EqualTo(job.Started));
            Assert.That(result.Completed, Is.EqualTo(job.Completed));
            Assert.That(result.State, Is.EqualTo(job.State));
            Assert.That(result.Error, Is.EqualTo(job.Error));

            Assert.That(result.Partitions, Is.Not.Null);
            Assert.That(result.Partitions.Untouched, Is.EqualTo(1L));
            Assert.That(result.Partitions.InProgress, Is.EqualTo(0L));
            Assert.That(result.Partitions.Completed, Is.EqualTo(0L));
            Assert.That(result.Partitions.Total, Is.EqualTo(1L));

            Assert.That(result.Progress, Is.Not.Null);
            Assert.That(result.Progress.Remaining, Is.EqualTo(26L));
            Assert.That(result.Progress.Completed, Is.EqualTo(0L));
            Assert.That(result.Progress.Total, Is.EqualTo(26L));
            Assert.That(result.Progress.Ratio, Is.EqualTo(0D));

            Assert.That(result.OwnershipChanges, Is.Zero);
            Assert.That(result.PendingSplitRequests, Is.Zero);
        }

        [Test]
        public async Task RetrieveJobStatusReportAsync_JobIsProcessing_ReturnsCorrectResult()
        {
            DateTime timestamp = new DateTime(2022, 9, 12, 17, 48, 41, DateTimeKind.Utc);

            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Processing);

            {
                Guid id = Guid.NewGuid();
                DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
                DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);
                Partition partition = new Partition(id, job.Id, created, updated, "80", "90", true, "82", 3L, 8L, "node-1", false, 5123.7, "node-6");

                await store.InsertPartitionAsync(partition, CancellationToken.None);
            }

            {
                Guid id = Guid.NewGuid();
                DateTime created = new DateTime(2022, 9, 12, 16, 33, 37, DateTimeKind.Utc);
                DateTime updated = new DateTime(2022, 9, 12, 17, 48, 31, DateTimeKind.Utc);
                Partition partition = new Partition(id, job.Id, created, updated, "10", "20", false, "13", 4L, 6L, "node-2", false, 4973.3, "node-7");

                await store.InsertPartitionAsync(partition, CancellationToken.None);
            }

            {
                Guid id = Guid.NewGuid();
                DateTime created = new DateTime(2022, 9, 12, 16, 25, 21, DateTimeKind.Utc);
                DateTime updated = new DateTime(2022, 9, 12, 17, 48, 30, DateTimeKind.Utc);
                Partition partition = new Partition(id, job.Id, created, updated, "20", "30", false, "14", 5L, 5L, "node-3", false, 3997.1, null);

                await store.InsertPartitionAsync(partition, CancellationToken.None);
            }

            {
                Guid id = Guid.NewGuid();
                DateTime created = new DateTime(2022, 9, 12, 17, 48, 31, DateTimeKind.Utc);
                DateTime updated = new DateTime(2022, 9, 12, 17, 48, 31, DateTimeKind.Utc);
                Partition partition = new Partition(id, job.Id, created, updated, "30", "40", false, null, 0L, 10L, null, false, 0.0, null);

                await store.InsertPartitionAsync(partition, CancellationToken.None);
            }

            {
                Guid id = Guid.NewGuid();
                DateTime created = new DateTime(2022, 9, 12, 17, 48, 29, DateTimeKind.Utc);
                DateTime updated = new DateTime(2022, 9, 12, 17, 48, 29, DateTimeKind.Utc);
                Partition partition = new Partition(id, job.Id, created, updated, "40", "50", false, null, 0L, 10L, null, false, 0.0, null);

                await store.InsertPartitionAsync(partition, CancellationToken.None);
            }

            {
                Guid id = Guid.NewGuid();
                DateTime created = new DateTime(2022, 9, 12, 17, 17, 16, DateTimeKind.Utc);
                DateTime updated = new DateTime(2022, 9, 12, 17, 48, 30, DateTimeKind.Utc);
                Partition partition = new Partition(id, job.Id, created, updated, "50", "60", false, "51", 2L, 8L, "node-4", false, 4997.9, null);

                await store.InsertPartitionAsync(partition, CancellationToken.None);
            }

            {
                Guid id = Guid.NewGuid();
                DateTime created = new DateTime(2022, 9, 12, 17, 17, 16, DateTimeKind.Utc);
                DateTime updated = new DateTime(2022, 9, 12, 17, 48, 30, DateTimeKind.Utc);
                Partition partition = new Partition(id, job.Id, created, updated, "60", "70", false, "69", 10L, 0L, "node-5", true, 5111.6, "node-8");

                await store.InsertPartitionAsync(partition, CancellationToken.None);
            }

            StatusReport result = await store.RetrieveJobStatusReportAsync(job.Id, timestamp, CancellationToken.None);

            Assert.That(result, Is.Not.Null);
            Assert.That(result.Id, Is.EqualTo(job.Id));
            Assert.That(result.Timestamp, Is.EqualTo(timestamp));
            Assert.That(result.Created, Is.EqualTo(job.Created));
            Assert.That(result.Updated, Is.EqualTo(job.Updated));
            Assert.That(result.BatchSize, Is.EqualTo(job.BatchSize));
            Assert.That(result.Timeout, Is.EqualTo(job.Timeout));
            Assert.That(result.Started, Is.EqualTo(job.Started));
            Assert.That(result.Completed, Is.EqualTo(job.Completed));
            Assert.That(result.State, Is.EqualTo(job.State));
            Assert.That(result.Error, Is.EqualTo(job.Error));

            Assert.That(result.Partitions, Is.Not.Null);
            Assert.That(result.Partitions.Untouched, Is.EqualTo(2L));
            Assert.That(result.Partitions.InProgress, Is.EqualTo(4L));
            Assert.That(result.Partitions.Completed, Is.EqualTo(1L));
            Assert.That(result.Partitions.Total, Is.EqualTo(7L));

            Assert.That(result.Progress, Is.Not.Null);
            Assert.That(result.Progress.Remaining, Is.EqualTo(47L));
            Assert.That(result.Progress.Completed, Is.EqualTo(24L));
            Assert.That(result.Progress.Total, Is.EqualTo(71L));
            Assert.That(result.Progress.Ratio, Is.EqualTo(0.338).Within(0.001D));

            Assert.That(result.CurrentThroughput, Is.EqualTo(13968.3).Within(0.1));
            Assert.That(result.PendingSplitRequests, Is.EqualTo(2));
        }

        [Test]
        public async Task RetrieveJobStatusReportAsync_JobIsCompleted_ReturnsCorrectResult()
        {
            DateTime timestamp = new DateTime(2022, 9, 12, 17, 48, 35, DateTimeKind.Utc);

            using EngineDataStoreWrapper store = await CreateEngineDataStoreAsync();

            Job job = await this.EnsurePersistedJobAsync(store, JobState.Completed);

            {
                Guid id = Guid.NewGuid();
                DateTime created = new DateTime(2022, 9, 12, 16, 22, 11, DateTimeKind.Utc);
                DateTime updated = new DateTime(2022, 9, 12, 16, 23, 31, DateTimeKind.Utc);
                Partition partition = new Partition(id, job.Id, created, updated, "80", "90", true, "90", 11L, 0L, "owner-1", true, 0.0, "owner-6");

                await store.InsertPartitionAsync(partition, CancellationToken.None);
            }

            {
                Guid id = Guid.NewGuid();
                DateTime created = new DateTime(2022, 9, 12, 16, 33, 37, DateTimeKind.Utc);
                DateTime updated = new DateTime(2022, 9, 12, 17, 48, 31, DateTimeKind.Utc);
                Partition partition = new Partition(id, job.Id, created, updated, "10", "20", false, "19", 10L, 0L, "owner-2", true, 0.0, "node-7");

                await store.InsertPartitionAsync(partition, CancellationToken.None);
            }

            {
                Guid id = Guid.NewGuid();
                DateTime created = new DateTime(2022, 9, 12, 16, 25, 21, DateTimeKind.Utc);
                DateTime updated = new DateTime(2022, 9, 12, 17, 48, 30, DateTimeKind.Utc);
                Partition partition = new Partition(id, job.Id, created, updated, "20", "30", false, "29", 10L, 0L, "owner-3", true, 0.0, null);

                await store.InsertPartitionAsync(partition, CancellationToken.None);
            }

            {
                Guid id = Guid.NewGuid();
                DateTime created = new DateTime(2022, 9, 12, 17, 48, 31, DateTimeKind.Utc);
                DateTime updated = new DateTime(2022, 9, 12, 17, 48, 31, DateTimeKind.Utc);
                Partition partition = new Partition(id, job.Id, created, updated, "30", "40", false, "39", 10L, 0L, "owner-1", true, 0.0, null);

                await store.InsertPartitionAsync(partition, CancellationToken.None);
            }

            {
                Guid id = Guid.NewGuid();
                DateTime created = new DateTime(2022, 9, 12, 17, 48, 29, DateTimeKind.Utc);
                DateTime updated = new DateTime(2022, 9, 12, 17, 48, 29, DateTimeKind.Utc);
                Partition partition = new Partition(id, job.Id, created, updated, "40", "50", false, "49", 10L, 0L, "owner-2", true, 0.0, null);

                await store.InsertPartitionAsync(partition, CancellationToken.None);
            }

            {
                Guid id = Guid.NewGuid();
                DateTime created = new DateTime(2022, 9, 12, 17, 17, 16, DateTimeKind.Utc);
                DateTime updated = new DateTime(2022, 9, 12, 17, 48, 30, DateTimeKind.Utc);
                Partition partition = new Partition(id, job.Id, created, updated, "50", "60", false, "59", 10L, 0L, "owner-4", true, 0.0, null);

                await store.InsertPartitionAsync(partition, CancellationToken.None);
            }

            {
                Guid id = Guid.NewGuid();
                DateTime created = new DateTime(2022, 9, 12, 17, 17, 16, DateTimeKind.Utc);
                DateTime updated = new DateTime(2022, 9, 12, 17, 48, 30, DateTimeKind.Utc);
                Partition partition = new Partition(id, job.Id, created, updated, "60", "70", false, "69", 10L, 0L, "owner-5", true, 0.0, null);

                await store.InsertPartitionAsync(partition, CancellationToken.None);
            }

            StatusReport result = await store.RetrieveJobStatusReportAsync(job.Id, timestamp, CancellationToken.None);

            Assert.That(result, Is.Not.Null);
            Assert.That(result.Id, Is.EqualTo(job.Id));
            Assert.That(result.Timestamp, Is.EqualTo(timestamp));
            Assert.That(result.Created, Is.EqualTo(job.Created));
            Assert.That(result.Updated, Is.EqualTo(job.Updated));
            Assert.That(result.BatchSize, Is.EqualTo(job.BatchSize));
            Assert.That(result.Timeout, Is.EqualTo(job.Timeout));
            Assert.That(result.Started, Is.EqualTo(job.Started));
            Assert.That(result.Completed, Is.EqualTo(job.Completed));
            Assert.That(result.State, Is.EqualTo(job.State));
            Assert.That(result.Error, Is.EqualTo(job.Error));

            Assert.That(result.Partitions, Is.Not.Null);
            Assert.That(result.Partitions.Untouched, Is.EqualTo(0L));
            Assert.That(result.Partitions.InProgress, Is.EqualTo(0L));
            Assert.That(result.Partitions.Completed, Is.EqualTo(7L));
            Assert.That(result.Partitions.Total, Is.EqualTo(7L));

            Assert.That(result.Progress, Is.Not.Null);
            Assert.That(result.Progress.Remaining, Is.EqualTo(0L));
            Assert.That(result.Progress.Completed, Is.EqualTo(71L));
            Assert.That(result.Progress.Total, Is.EqualTo(71L));
            Assert.That(result.Progress.Ratio, Is.EqualTo(1D));

            Assert.That(result.CurrentThroughput, Is.EqualTo(0D));
            Assert.That(result.PendingSplitRequests, Is.EqualTo(0));
        }


        [DebuggerStepThrough]
        private class EngineDataStoreWrapper : IEngineDataStore, IDisposable, IAsyncDisposable
        {
            private readonly IEngineDataStore _store;

            public EngineDataStoreWrapper(IEngineDataStore store)
            {
                Debug.Assert(store != null);

                this._store = store;
            }

            public void Dispose()
            {
                IDisposable disposable = (this._store as IDisposable);

                if (disposable != null)
                {
                    disposable.Dispose();

                    GC.SuppressFinalize(this);
                }
            }

            public async ValueTask DisposeAsync()
            {
                IAsyncDisposable disposable = (this._store as IAsyncDisposable);

                if (disposable != null)
                {
                    await disposable.DisposeAsync();

                    GC.SuppressFinalize(this);
                }
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

            public Task<Partition> ReportProgressAsync(Guid id, string owner, DateTime timestamp, string position, long processed, long remaining, bool completed, double throughput, CancellationToken cancellation)
                => this._store.ReportProgressAsync(id, owner, timestamp, position, processed, remaining, completed, throughput, cancellation);

            public Task<Job> RetrieveJobAsync(string id, CancellationToken cancellation)
                => this._store.RetrieveJobAsync(id, cancellation);

            public Task<Partition> RetrievePartitionAsync(Guid id, CancellationToken cancellation)
                => this._store.RetrievePartitionAsync(id, cancellation);

            public Task<Partition> TryAcquirePartitionAsync(string jobId, string requester, DateTime timestamp, DateTime active, CancellationToken cancellation)
                => this._store.TryAcquirePartitionAsync(jobId, requester, timestamp, active, cancellation);

            public Task<bool> TryRequestSplitAsync(string jobId, string requester, DateTime active, CancellationToken cancellation)
                => this._store.TryRequestSplitAsync(jobId, requester, active, cancellation);

            public Task<bool> IsSplitRequestPendingAsync(string jobId, string requester, CancellationToken cancellation)
                => this._store.IsSplitRequestPendingAsync(jobId, requester, cancellation);

            public Task<Job> MarkJobAsReadyAsync(string id, DateTime timestamp, CancellationToken cancellation)
                => this._store.MarkJobAsReadyAsync(id, timestamp, cancellation);

            public Task<Job> MarkJobAsStartedAsync(string id, DateTime timestamp, CancellationToken cancellation)
                => this._store.MarkJobAsStartedAsync(id, timestamp, cancellation);

            public Task<Job> MarkJobAsCompletedAsync(string id, DateTime timestamp, CancellationToken cancellation)
                => this._store.MarkJobAsCompletedAsync(id, timestamp, cancellation);

            public Task<Job> MarkJobAsFailedAsync(string id, DateTime timestamp, string error, CancellationToken cancellation)
                => this._store.MarkJobAsFailedAsync(id, timestamp, error, cancellation);

            public Task<StatusReport> RetrieveJobStatusReportAsync(string id, DateTime timestamp, CancellationToken cancellation)
                => this._store.RetrieveJobStatusReportAsync(id, timestamp, cancellation);
        }
    }
}
