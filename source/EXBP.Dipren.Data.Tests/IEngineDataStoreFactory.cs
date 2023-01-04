
namespace EXBP.Dipren.Data.Tests
{
    public interface IEngineDataStoreFactory
    {
        public Task<IEngineDataStore> CreateAsync(CancellationToken cancellation);
    }
}
