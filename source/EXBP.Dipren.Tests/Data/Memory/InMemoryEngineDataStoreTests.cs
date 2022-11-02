
using EXBP.Dipren.Data.Memory;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Data.Memory
{
    [TestFixture]
    public class InMemoryEngineDataStoreTests : EngineDataStoreTests<InMemoryEngineDataStore>
    {
        protected override Task<InMemoryEngineDataStore> CreateEngineDataStoreAsync()
            => Task.FromResult(new InMemoryEngineDataStore());
    }
}
