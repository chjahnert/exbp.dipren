
using System.Diagnostics;

using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Defines a distributed processing job.
    /// </summary>
    /// <typeparam name="TKey">
    ///   The type of the key that identifies the items to be processed.
    /// </typeparam>
    /// <typeparam name="TItem">
    ///   The type of the items to process.
    /// </typeparam>
    [DebuggerDisplay("Name = {Name}")]
    public class Job<TKey, TItem> where TKey : IComparable<TKey>
    {
        private readonly Guid _id;
        private readonly string _name;
        private readonly IDataSource<TKey, TItem> _source;
        private readonly IKeyArithmetics<TKey> _arithmetics;
        private readonly IKeySerializer<TKey> _serializer;
        private readonly IBatchProcessor<TItem> _processor;
        private readonly TimeSpan _timeout;
        private readonly int _batchSize;


        /// <summary>
        ///   Gets the unique identifier of the current distributed processing job.
        /// </summary>
        /// <value>
        ///   A <see cref="Guid"/> value that contains the unique identifier of the current distributed processing job.
        /// </value>
        public Guid Id => this._id;

        /// <summary>
        ///   Gets the name of the current distributed processing job.
        /// </summary>
        /// <value>
        ///   A <see cref="string"/> value that is the name of the current distributed processing job.
        /// </value>
        /// <remarks>
        ///   The name is used to uniquely identify the ranges in the range set that are related to the current job.
        /// </remarks>
        public string Name => this._name;

        /// <summary>
        ///   Gets the data source that is used to access the entries to be processed.
        /// </summary>
        /// <value>
        ///   An <see cref="IDataSource{TKey, TValue}"/> object that is used to access the entries to be processed.
        /// </value>
        public IDataSource<TKey, TItem> Source => this._source;

        /// <summary>
        ///   Gets the key arithmetics provider to use to manipulate key ranges.
        /// </summary>
        /// <value>
        ///   An <see cref="IKeyArithmetics{TKey}"/> object that is used to manipulate key ranges.
        /// </value>
        public IKeyArithmetics<TKey> Arithmetics => this._arithmetics;

        /// <summary>
        ///   Gets the serializer to use to convert keys to their string representation and back.
        /// </summary>
        /// <value>
        ///   A <see cref="IKeyArithmetics{TKey}"/> object that can convert between keys and their string
        ///   representations.
        /// </value>
        public IKeySerializer<TKey> Serializer => this._serializer;

        /// <summary>
        ///   Gets the object that processes batches of entries.
        /// </summary>
        /// <value>
        ///   An <see cref="IBatchProcessor{TEntry}"/> object that is used to process the entries.
        /// </value>
        public IBatchProcessor<TItem> Processor => this._processor;

        /// <summary>
        ///   Gets the time after which a range is considered stalled or abandoned.
        /// </summary>
        /// <value>
        ///   A <see cref="TimeSpan"/> value that contains the time after which a range is considered stalled or
        ///   abandoned.
        /// </value>
        public TimeSpan Timeout => this._timeout;

        /// <summary>
        ///   Gets the maximum number of entries to include in a batch.
        /// </summary>
        /// <value>
        ///   An <see cref="int"/> value that is the maximum number of entries to include in a batch.
        /// </value>
        public int BatchSize => this._batchSize;


        /// <summary>
        ///   Initializes a new instance of the <see cref="Job{TKey, TValue}"/> class.
        /// </summary>
        /// <param name="name">
        ///   The unique name of the distributed processing job.
        /// </param>
        /// <param name="source">
        ///   The <see cref="IDataSource{TKey, TValue}"/> object to use to access the entries to be processed.
        /// </param>
        /// <param name="arithmetics">
        ///   The <see cref="IKeyArithmetics{TKey}"/> object to use to manipulate key ranges.
        /// </param>
        /// <param name="serializer">
        ///   The <see cref="IKeyArithmetics{TKey}"/> object to use to convert between key values and their string
        ///   representation.
        /// </param>
        /// <param name="processor">
        ///   The <see cref="IBatchProcessor{TEntry}"/> object to use to process the entries.
        /// </param>
        /// <param name="timeout">
        ///   The time after which a range is considered stalled or abandoned.
        /// </param>
        /// <param name="batchSize">
        ///   The maximum number of entries to include in a batch.
        /// </param>
        public Job(string name, IDataSource<TKey, TItem> source, IKeyArithmetics<TKey> arithmetics, IKeySerializer<TKey> serializer, IBatchProcessor<TItem> processor, TimeSpan timeout, int batchSize)
        {
            Assert.ArgumentIsNotNull(name, nameof(name));
            Assert.ArgumentIsNotEmpty(name, false, nameof(name));
            Assert.ArgumentIsNotNull(source, nameof(source));
            Assert.ArgumentIsNotNull(arithmetics, nameof(arithmetics));
            Assert.ArgumentIsNotNull(serializer, nameof(serializer));
            Assert.ArgumentIsNotNull(processor, nameof(processor));
            Assert.ArgumentIsGreater(timeout, TimeSpan.Zero, nameof(timeout));
            Assert.ArgumentIsGreater(batchSize, 0, nameof(batchSize));

            this._id = Guid.NewGuid();
            this._name = name;
            this._source = source;
            this._arithmetics = arithmetics;
            this._serializer = serializer;
            this._processor = processor;
            this._timeout = timeout;
            this._batchSize = batchSize;
        }
    }
}
