
namespace EXBP.Dipren.Demo.SQLite
{
    internal record Measurement
    {
        public int Id { get; init; }

        public int Width { get; init; }

        public int Height { get; init; }

        public int Depth { get; init; }
    }
}
