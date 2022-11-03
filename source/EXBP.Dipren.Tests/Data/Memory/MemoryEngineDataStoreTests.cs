
using EXBP.Dipren.Data.Memory;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Data.Memory
{
    [TestFixture]
    public class MemoryEngineDataStoreTests : EngineDataStoreTests<MemoryEngineDataStore>
    {
        protected override Task<MemoryEngineDataStore> CreateEngineDataStoreAsync()
            => Task.FromResult(new MemoryEngineDataStore());
    }
}
