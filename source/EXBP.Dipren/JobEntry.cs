
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Represents a persisted <see cref="Job{TKey, TItem}"/> object.
    /// </summary>
    public record JobEntry
    {
        private readonly Guid _id;
        private readonly string _name;
        private readonly DateTime _created;
        private readonly DateTime _updated;
        private readonly JobState _state;


        /// <summary>
        ///   Gets the unique identifier of the current partition.
        /// </summary>
        /// <value>
        ///   A <see cref="Guid"/> value that is the unique identifier of the current partition.
        /// </value>
        public Guid Id =>  this._id;

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
        ///   Initializes a new instance of the <see cref="PartitionEntry"/> record.
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
        public JobEntry(Guid id, string name, DateTime created, DateTime updated, JobState state)
        {
            Assert.ArgumentIsNotNull(name, nameof(name));
            Assert.ArgumentIsDefined(state, nameof(state));

            this._id = id;
            this._name = name;
            this._created = created;
            this._updated = updated;
            this._state = state;
        }
    }
}
