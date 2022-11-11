﻿namespace EXBP.Dipren.Demo.Postgres.Processing.Models
{
    internal record Cuboid
    {
        public Guid Id { get; init; }

        public int Width { get; init; }

        public int Height { get; init; }

        public int Depth { get; init; }
    }
}