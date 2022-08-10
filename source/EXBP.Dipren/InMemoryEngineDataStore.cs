
using System.Collections.ObjectModel;

using EXBP.Dipren.Data;
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
            ///   Initializes a new instance of the <see cref="Job"/> class.
            /// </summary>
            /// <param name="name">
            ///   The unique name of the job.
            /// </param>
            public Job(string name)
            {
                this.Name = name;
            }   
        }
    }
}
