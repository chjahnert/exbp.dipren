
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;


namespace EXBP.Dipren.Demo.SQLite
{
    internal class MeasurementBatchProcessor : IBatchProcessor<Measurement>
    {
        private readonly object _syncRoot = new object();
        private readonly string _connectionString;
        private readonly List<Entry> _buffer;


        public MeasurementBatchProcessor(string connectionString, int bufferSize)
        {
            Debug.Assert(bufferSize > 0);

            this._buffer = new List<Entry>(bufferSize);
            this._connectionString = connectionString;
        }


        public Task ProcessAsync(IEnumerable<Measurement> measurements, CancellationToken cancellation)
        {
            IEnumerable<Entry> entries = measurements.Select(measurement => new Entry
            {
                Id = measurement.Id,
                Volume = (measurement.Width * measurement.Height * measurement.Depth)
            });

            lock (this._syncRoot)
            {
                foreach (Entry entry in entries)
                {
                    this._buffer.Add(entry);

                    if (this._buffer.Count == this._buffer.Capacity)
                    {
                        this.Flush();
                    }
                }
            }

            return Task.CompletedTask;
        }

        public void Flush()
        {
            if (this._buffer.Count > 0)
            {
                using (SQLiteConnection connection = this.OpenDatabaseConnection())
                {
                    using SQLiteTransaction transaction = connection.BeginTransaction();

                    using SQLiteCommand command = new SQLiteCommand
                    {
                        CommandText = MeasurementBatchProcessorResources.QueryInsertVolume,
                        CommandType = CommandType.Text,
                        Transaction = transaction
                    };

                    SQLiteParameter parameterId = command.Parameters.Add("$id", DbType.Int32);
                    SQLiteParameter parameterVolume = command.Parameters.Add("$volume", DbType.Int32);

                    foreach (Entry entry in this._buffer)
                    {
                        parameterId.Value = entry.Id;
                        parameterVolume.Value = entry.Volume;

                        command.ExecuteNonQuery();
                    }

                    transaction.Commit();
                }

                this._buffer.Clear();
            }
        }

        private SQLiteConnection OpenDatabaseConnection()
        {
            SQLiteConnection result = new SQLiteConnection(this._connectionString);

            result.Open();

            using SQLiteCommand commandSchema = new SQLiteCommand
            {
                CommandText = MeasurementBatchProcessorResources.QueryCreateSchema,
                CommandType = CommandType.Text,
                Connection = result
            };

            commandSchema.ExecuteNonQuery();

            return result;
        }


        private record Entry
        {
            public int Id { get; init; }

            public int Volume { get; init; }
        }
    }
}
