
using System.Collections.ObjectModel;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements an in-memory <see cref="IEngineDataStore"/> that can be used for testing.
    /// </summary>
    public class InMemoryEngineDataStore
    {
        private readonly object _syncRoot = new object();
        private readonly JobCollection _jobs = new JobCollection();


        /// <summary>
        ///   Initializes a new and empty instance of the <see cref="InMemoryEngineDataStore"/> class.
        /// </summary>
        public InMemoryEngineDataStore()
        {
        }

        /// <summary>
        ///   Returns the number of distributed processing jobs in the current data store.
        /// </summary>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> or <see cref="long"/> that represents the asynchronous operation and can
        ///   be used to access the result.
        /// </returns>
        public Task<long> CountJobsAsync(CancellationToken cancellation)
        {
            long result = 0L;

            lock (this._syncRoot)
            {
                result = this._jobs.Count;
            }

            return Task.FromResult(result);
        }

        /// <summary>
        ///   Inserts a job with the specified details into the current data store.
        /// </summary>
        /// <param name="name">
        ///   The unique name of the distributed processing job.
        /// </param>
        /// <param name="state">
        ///   The state of the new distributed processing job.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> that represents the asynchronous operation.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///   Argument <paramref name="name"/> is a <see langword="null"/> reference.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///   Argument <paramref name="state"/> contains value that is not defined; or a distributed processing job
        ///   with the same name already exists in the current data store.
        /// </exception>
        public Task InsertJobAsync(string name, JobState state, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(name, nameof(name));
            Assert.ArgumentIsDefined(state, nameof(state));

            Job job = new Job(name, state);

            lock (this._syncRoot)
            {
                this._jobs.Add(job);
            }

            return Task.CompletedTask;
        }

        /// <summary>
        ///   Sets the state of a job to the specified value.
        /// </summary>
        /// <param name="name">
        ///   The unique name of the job to update.
        /// </param>
        /// <param name="state">
        ///   The new state to set.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task"/> that represents the asynchronous operation.
        /// </returns>
        public Task SetJobStateAsync(string name, JobState state, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(name, nameof(name));

            Job job = null;
            
            lock (this._syncRoot)
            {
                job = this._jobs[name];
            }

            lock (job)
            {
                job.State = state;
            }

            return Task.CompletedTask;
        }

        /// <summary>
        ///   Returns the current state of a job.
        /// </summary>
        /// <param name="name">
        ///   The unique name of the job.
        /// </param>
        /// <param name="cancellation">
        ///   The <see cref="CancellationToken"/> used to propagate notifications that the operation should be
        ///   canceled.
        /// </param>
        /// <returns>
        ///   A <see cref="Task{TResult}"/> of <see cref="JobState"/> object that represents the asynchronous operation
        ///   and can be used to access the result.
        /// </returns>
        public Task<JobState> GetJobStateAsync(string name, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(name, nameof(name));

            Job job = null;

            lock (this._syncRoot)
            {
                job = this._jobs[name];
            }

            JobState result = default;

            lock (job)
            {
                result = job.State;
            }

            return Task.FromResult(result);
        }

        /// <summary>
        ///   Implements a collection of <see cref="Job"/> objects.
        /// </summary>
        private class JobCollection : KeyedCollection<string, Job>
        {
            /// <summary>
            ///   Initializes a new and empty instance of the <see cref="JobCollection"/> type.
            /// </summary>
            public JobCollection() : base(StringComparer.Ordinal)
            {
            }

            /// <summary>
            ///   Returns the key for the specified item.
            /// </summary>
            /// <param name="item">
            ///   The item for which to return the key.
            /// </param>
            /// <returns>
            ///   The key of the specified item.
            /// </returns>
            protected override string GetKeyForItem(Job item)
                => item.Name;
        }

        /// <summary>
        ///   Represents a distributed processing job in an <see cref="InMemoryEngineDataStore"/> instance.
        /// </summary>
        private sealed class Job
        {
            /// <summary>
            ///   Gets the unique name of the current distributed processing job.
            /// </summary>
            /// <value>
            ///   A <see cref="string"/> value containing the unique name of the current distributed processing job.
            /// </value>
            public string Name { get; }

            /// <summary>
            ///   Gets or sets a value that indicates the state of the current distributed processing job.
            /// </summary>
            /// <value>
            ///   A <see cref="JobState"/> value that indicates the state of the current distributed processing job.
            /// </value>
            public JobState State { get; set; }


            /// <summary>
            ///   Initializes a new instance of the <see cref="Job"/> class.
            /// </summary>
            /// <param name="name">
            ///   The unique name of the job.
            /// </param>
            /// <param name="state">
            ///   The initial state of the job.
            /// </param>
            public Job(string name, JobState state)
            {
                this.Name = name;
                this.State = state;
            }   
        }
    }
}
