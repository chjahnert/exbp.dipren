
namespace EXBP.Dipren.Data.Tests
{
    public interface IEngineDataStoreFactory
    {
        Task<IEngineDataStore> CreateAsync(CancellationToken cancellation);
    }
}
