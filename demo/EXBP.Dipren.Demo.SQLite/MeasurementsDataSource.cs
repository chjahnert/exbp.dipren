
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Diagnostics;


namespace EXBP.Dipren.Demo.SQLite
{
    internal class MeasurementsDataSource : IDataSource<int, Measurement>
    {
        private readonly SQLiteConnection _connection;


        public MeasurementsDataSource(SQLiteConnection connection)
        {
            Debug.Assert(connection != null);

            this._connection = connection;
        }


        public async Task<long> EstimateRangeSizeAsync(Range<int> range, CancellationToken canellation)
        {
            Debug.Assert(range != null);

            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = MeasurementDataSourceResources.QueryEstimateRangeSize,
                CommandType = CommandType.Text,
                Connection = this._connection
            };

            command.Parameters.AddWithValue("$first", range.First);
            command.Parameters.AddWithValue("$last", range.Last);
            command.Parameters.AddWithValue("$inclusive", range.IsInclusive);

            long result = (long) await command.ExecuteScalarAsync(canellation);

            return result;
        }

        public async Task<Range<int>> GetEntireRangeAsync(CancellationToken cancellation)
        {
            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = MeasurementDataSourceResources.QueryGetEntireRangeAscending,
                CommandType = CommandType.Text,
                Connection = this._connection
            };

            Range<int> result = null;

            using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
            {
                reader.Read();

                int first = reader.GetInt32("first");
                int last = reader.GetInt32("last");

                result = new Range<int>(first, last, true);
            }

            return result;
        }

        public async Task<IEnumerable<KeyValuePair<int, Measurement>>> GetNextBatchAsync(Range<int> range, int skip, int take, CancellationToken cancellation)
        {
            using SQLiteCommand command = new SQLiteCommand
            {
                CommandText = MeasurementDataSourceResources.QueryGetNextBatchAscending,
                CommandType = CommandType.Text,
                Connection = this._connection
            };

            command.Parameters.AddWithValue("$first", range.First);
            command.Parameters.AddWithValue("$last", range.Last);
            command.Parameters.AddWithValue("$inclusive", range.IsInclusive);
            command.Parameters.AddWithValue("$skip", skip);
            command.Parameters.AddWithValue("$take", take);

            List<KeyValuePair<int, Measurement>> result = new List<KeyValuePair<int, Measurement>>(take);

            using (DbDataReader reader = await command.ExecuteReaderAsync(cancellation))
            {
                while (reader.Read() == true)
                {
                    Measurement measurement = new Measurement
                    {
                        Id = reader.GetInt32("id"),
                        Width = reader.GetInt32("width"),
                        Height = reader.GetInt32("height"),
                        Depth = reader.GetInt32("depth")
                    };

                    KeyValuePair<int, Measurement> item = new KeyValuePair<int, Measurement>(measurement.Id, measurement);

                    result.Add(item);
                }
            }

            return result;
        }
    }
}
