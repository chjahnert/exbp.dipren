
namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements an in-memory <see cref="IEngineDataStore"/> that can be used for testing.
    /// </summary>
    public class InMemoryEngineDataStore
    {
        /// <summary>
        ///   Initializes a new and empty instance of the <see cref="InMemoryEngineDataStore"/> class.
        /// </summary>
        public InMemoryEngineDataStore()
        {
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
