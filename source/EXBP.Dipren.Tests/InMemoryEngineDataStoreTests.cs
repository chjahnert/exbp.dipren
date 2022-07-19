
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class InMemoryEngineDataStoreTests
    {
        private const string JOB_NAME_FIRST = "first";
        private const string JOB_NAME_SECOND = "second";


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

            await store.InsertJobAsync(JOB_NAME_FIRST, JobState.Initializing, CancellationToken.None);
            await store.InsertJobAsync(JOB_NAME_SECOND, JobState.Initializing, CancellationToken.None);

            long count = await store.CountJobsAsync(CancellationToken.None);

            Assert.That(count, Is.EqualTo(2L));
        }

        [Test]
        public async Task InsertJobAsync_JobWithSameNameAreadyExists_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            await store.InsertJobAsync(JOB_NAME_FIRST, JobState.Initializing, CancellationToken.None);

            Assert.ThrowsAsync<ArgumentException>(async () => await store.InsertJobAsync(JOB_NAME_FIRST, JobState.Initializing, CancellationToken.None));
        }

        [Test]
        public void SetJobStateAsync_JobWithSpecifiedNameDoesNotExists_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Assert.ThrowsAsync<KeyNotFoundException>(async () => await store.SetJobStateAsync(JOB_NAME_FIRST, JobState.Ready, CancellationToken.None));
        }

        [Test]
        public async Task SetJobStateAsync_JobExists_StateIsUpdated()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            await store.InsertJobAsync(JOB_NAME_FIRST, JobState.Initializing, CancellationToken.None);
            await store.SetJobStateAsync(JOB_NAME_FIRST, JobState.Ready, CancellationToken.None);

            JobState state = await store.GetJobStateAsync(JOB_NAME_FIRST, CancellationToken.None);

            Assert.That(state, Is.EqualTo(JobState.Ready));
        }

        [Test]
        public void GetJobStateAsync_JobWithSpecifiedNameDoesNotExists_ThrowsException()
        {
            InMemoryEngineDataStore store = new InMemoryEngineDataStore();

            Assert.ThrowsAsync<KeyNotFoundException>(async () => await store.GetJobStateAsync(JOB_NAME_FIRST, CancellationToken.None));
        }
    }
}
