
namespace EXBP.Dipren.Demo.SQLite
{
    internal class MeasurementBatchProcessor : IBatchProcessor<Measurement>
    {
        public async Task ProcessAsync(IEnumerable<Measurement> items, CancellationToken cancellation)
        {
            await Task.Delay(10);
        }
    }
}
