
namespace EXBP.Dipren
{
    /// <summary>
    ///   Represents a distributed processing job.
    /// </summary>
    /// <typeparam name="TKey">
    ///   The type of the key that identifies the entries to be processed.
    /// </typeparam>
    public class JobDefinition<TKey>
    {
        private readonly string _name;
        private readonly string _description;
        private readonly RangeDefinition<TKey> _range;


        /// <summary>
        ///   Gets the name of the current distributed processing job.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that is the name of the current distributed processing job.
        /// </value>
        public string Name => this._name;

        /// <summary>
        ///   Gets the description of the current distributed processing job.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that contains the description of the current distributed processing job.
        /// </value>
        public string Description => this._description;

        /// <summary>
        ///   Gets the key at which to start processing.
        /// </summary>
        /// <value>
        ///   A <see cref="TKey"/> value that is the key at which to start processing.
        /// </value>
        public RangeDefinition<TKey> Range => this._range;

        /// <summary>
        ///   Initializes a new instance of the <see cref="JobDefinition{TKey}"/> class.
        /// </summary>
        /// <param name="name">
        ///   The name of the distributed processing job.
        /// </param>
        /// <param name="description">
        ///   The description of the distributed processing job or <see langword="null"/>.
        /// </param>
        /// <param name="range">
        ///   The key range to process.
        /// </param>
        public JobDefinition(string name, string description, RangeDefinition<TKey> range)
        {
            this._name = name;
            this._description = description;
            this._range = range;
        }   
    }
}
