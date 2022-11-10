
using EXBP.Dipren.Demo.Postgres.Processing.Models;


namespace EXBP.Dipren.Demo.Postgres.Processing
{
    internal class CubiodBatchProcessor : IBatchProcessor<Cuboid>
    {
        public Task ProcessAsync(IEnumerable<Cuboid> items, CancellationToken cancellation)
        {
            throw new NotImplementedException();
        }
    }
}
