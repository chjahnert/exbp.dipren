
using System.Diagnostics;


namespace EXBP.Dipren.Data.Tests
{
    public abstract class EngineDataStoreIntegrationTests
    {
        [DebuggerDisplay("ID = {Id}, Dimensions = {Width} × {Height} × {Depth} ")]
        protected class Cuboid
        {
            public int Id { get; set; }

            public float Width { get; set; }

            public float Height { get; set; }

            public float Depth { get; set; }
        }
    }
}
