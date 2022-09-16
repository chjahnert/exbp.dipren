
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren.Data
{
    /// <summary>
    ///   Represents a persisted <see cref="Job{TKey, TItem}"/> object.
    /// </summary>
    public record Job
    {
        private readonly Guid _id;
        private readonly string _name;
        private readonly DateTime _created;
        private readonly DateTime _updated;
        private readonly JobState _state;
        private readonly Exception _exception;


        /// <summary>
        ///   Gets the unique identifier of the current partition.
        /// </summary>
        /// <value>
        ///   A <see cref="Guid"/> value that is the unique identifier of the current partition.
        /// </value>
        public Guid Id => this._id;

        /// <summary>
        ///   Gets the name of the current job.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains the name of the current job.
        /// </value>
        public string Name => this._name;

        /// <summary>
        ///   Gets the date and time when the current job was created.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value that contains the date and time, in UTC, the current job was created.
        /// </value>
        public DateTime Created => this._created;

        /// <summary>
        ///   Gets the date and time when the current job was last updated.
        /// </summary>
        /// <value>
        ///   A <see cref="DateTime"/> value that contains the date and time, in UTC, the current job was last updated.
        /// </value>
        public DateTime Updated => this._updated;

        /// <summary>
        ///   Gets a value indicating the state of the current job.
        /// </summary>
        /// <value>
        ///   A <see cref="JobState"/> value indicating the state of the current job.
        /// </value>
        public JobState State => this._state;

        /// <summary>
        ///   Gets the exception that is the reason the current job failed.
        /// </summary>
        /// <value>
        ///   A <see cref="Exception"/> object that provides information about the error that occurred; or
        ///   <see langword="null"/> if no error occurred.
        /// </value>
        public Exception Exception => this._exception;


        /// <summary>
        ///   Initializes a new instance of the <see cref="Partition"/> record.
        /// </summary>
        /// <param name="id">
        ///   The unique identifier of the job.
        /// </param>
        /// <param name="name">
        ///   The name of the job.
        /// </param>
        /// <param name="created">
        ///   The date and time when the job was created.
        /// </param>
        /// <param name="updated">
        ///   The date and time when the job was last updated.
        /// </param>
        /// <param name="state">
        ///   The state of the job.
        /// </param>
        /// <param name="exception">
        ///   The exception that is the reason the job failed.
        /// </param>
        public Job(Guid id, string name, DateTime created, DateTime updated, JobState state, Exception exception = null)
        {
            Assert.ArgumentIsNotNull(name, nameof(name));
            Assert.ArgumentIsDefined(state, nameof(state));

            this._id = id;
            this._name = name;
            this._created = created;
            this._updated = updated;
            this._state = state;
            this._exception = exception;
        }
    }
}
