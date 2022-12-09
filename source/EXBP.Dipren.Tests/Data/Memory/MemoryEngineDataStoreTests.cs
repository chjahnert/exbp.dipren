
using EXBP.Dipren.Data;
using EXBP.Dipren.Data.Memory;
using EXBP.Dipren.Data.Tests;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Data.Memory
{
    [TestFixture]
    public class MemoryEngineDataStoreTests : EngineDataStoreTests
    {
        protected override Task<IEngineDataStore> OnCreateEngineDataStoreAsync()
            => Task.FromResult<IEngineDataStore>(new MemoryEngineDataStore());
    }
}
