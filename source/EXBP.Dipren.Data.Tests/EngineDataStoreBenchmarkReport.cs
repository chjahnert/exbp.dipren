
using System.Drawing;
using System.Globalization;
using System.Text.Json;

using EXBP.Dipren.Data.Tests;
using EXBP.Dipren.Diagnostics;

using ScottPlot;
using ScottPlot.Plottable;


namespace EXBP.Dipren.Data.Postgres.Tests
{
    public class EngineDataStoreBenchmarkReport
    {
        private readonly string _outputDirectory;
        private readonly int _plotWith;
        private readonly int _plotHeight;
        private readonly float _lineWidth = 1.0f;


        public EngineDataStoreBenchmarkReport(string outputDirectory, int plotWith = 1024, int plotHeight = 768)
        {
            Assert.ArgumentIsNotNull(outputDirectory, nameof(outputDirectory));
            Assert.ArgumentIsGreater(plotWith, 0, nameof(plotWith));
            Assert.ArgumentIsGreater(plotHeight, 0, nameof(plotHeight));

            this._outputDirectory = outputDirectory;
            this._plotWith = plotWith;
            this._plotHeight = plotHeight;
        }


        public virtual async Task<string> GenerateReportAsync(EngineDataStoreBenchmarkResult recording, CancellationToken cancellation = default)
        {
            Assert.ArgumentIsNotNull(recording, nameof(recording));

            string result = this.EnsureOutputDirectory(recording);

            await this.WriteDatasetAsync(recording, result, cancellation);

            this.PlotPartitions(recording, result);
            this.PlotEstimatedTime(recording, result);
            this.PlotThroughput(recording, result);
            this.PlotProgress(recording, result);

            return result;
        }

        protected string EnsureOutputDirectory(EngineDataStoreBenchmarkResult recording)
        {
            Assert.ArgumentIsNotNull(recording, nameof(recording));

            string result = Path.Combine(this._outputDirectory, recording.Id);

            Directory.CreateDirectory(result);

            return result;
        }

        protected virtual async Task WriteDatasetAsync(EngineDataStoreBenchmarkResult recording, string directory, CancellationToken cancellation)
        {
            Assert.ArgumentIsNotNull(directory, nameof(directory));
            Assert.ArgumentIsNotNull(recording, nameof(recording));

            string fileName = Path.Combine(directory, "output" + ".json");

            using (FileStream writer = new FileStream(fileName, FileMode.CreateNew))
            {
                JsonSerializerOptions options = new JsonSerializerOptions
                {
                    WriteIndented = true
                };

                await JsonSerializer.SerializeAsync(writer, recording, options);
            }
        }

        protected virtual void PlotPartitions(EngineDataStoreBenchmarkResult recording, string directory)
        {
            Assert.ArgumentIsNotNull(directory, nameof(directory));
            Assert.ArgumentIsNotNull(recording, nameof(recording));

            Plot plot = new Plot(this._plotWith, this._plotHeight);

            DateTime start = recording.Snapshots.Min(s => s.Timestamp);
            double[] timestamps = recording.Snapshots.Select(s => (s.Timestamp - start).TotalSeconds).ToArray();

            double[] pending = recording.Snapshots.Select(s => (double) s.Partitions.Untouched).ToArray();
            double[] completed = recording.Snapshots.Select(s => (double) s.Partitions.Completed).ToArray();
            double[] active = recording.Snapshots.Select(s => (double) s.Partitions.InProgress).ToArray();
            double[] total = recording.Snapshots.Select(s => (double) s.Partitions.Total).ToArray();

            ScatterPlot scatterPending = plot.AddScatter(timestamps, pending, Color.Blue, this._lineWidth, 5, MarkerShape.none, LineStyle.Solid, "Pending");
            ScatterPlot scatterCompleted = plot.AddScatter(timestamps, completed, Color.Red, this._lineWidth, 5, MarkerShape.none, LineStyle.Solid, "Completed");
            ScatterPlot scatterActive = plot.AddScatter(timestamps, active, Color.Green, this._lineWidth, 5, MarkerShape.none, LineStyle.Solid, "Active");
            ScatterPlot scatterTotal = plot.AddScatter(timestamps, total, Color.DarkGray, this._lineWidth, 5, MarkerShape.none, LineStyle.Solid, "Total");

            plot.Title("Partitions");
            plot.Legend(true, Alignment.UpperLeft);

            this.SaveImage(plot, directory, "partitions");
        }

        protected virtual void PlotEstimatedTime(EngineDataStoreBenchmarkResult recording, string directory)
        {
            Assert.ArgumentIsNotNull(directory, nameof(directory));
            Assert.ArgumentIsNotNull(recording, nameof(recording));

            Plot plot = new Plot(this._plotWith, this._plotHeight);

            DateTime start = recording.Snapshots.Min(s => s.Timestamp);
            TimeSpan runtime = (recording.Snapshots.Max(s => s.Timestamp) - start);

            double[] timestamps = recording.Snapshots.Select(s => (s.Timestamp - start).TotalSeconds).ToArray();
            double[] estimation = recording.Snapshots.Select(s => ((s.Progress.Remaining != null) && (s.CurrentThroughput > 0.0)) ? TimeSpan.FromSeconds(s.Progress.Remaining.Value / s.CurrentThroughput).TotalSeconds : 0.0).ToArray();
            double[] inaccuracy = recording.Snapshots.Select(s => Math.Abs((((s.Progress.Remaining != null) && (s.CurrentThroughput > 0.0)) ? TimeSpan.FromSeconds(s.Progress.Remaining.Value / s.CurrentThroughput).TotalSeconds : 0.0)- (runtime - (s.Timestamp - start)).TotalSeconds)).ToArray();

            ScatterPlot scatterInaccuracy = plot.AddScatter(timestamps, inaccuracy, Color.Red, this._lineWidth, 5, MarkerShape.none, LineStyle.Solid, "Inaccuracy (sec)");
            ScatterPlot scatterEstimation = plot.AddScatter(timestamps, estimation, Color.Blue, this._lineWidth, 5, MarkerShape.none, LineStyle.Solid, "Estimation (sec)");

            plot.Title("Estimated Time to Completion via Throughput");
            plot.Legend(true, Alignment.UpperRight);
            plot.XAxis.Label("Time (sec)");
            plot.YAxis.Label("Estimate (sec)");

            this.SaveImage(plot, directory, "estimations");
        }

        protected virtual void PlotThroughput(EngineDataStoreBenchmarkResult recording, string directory)
        {
            Assert.ArgumentIsNotNull(directory, nameof(directory));
            Assert.ArgumentIsNotNull(recording, nameof(recording));

            Plot plot = new Plot(this._plotWith, this._plotHeight);

            DateTime start = recording.Snapshots.Min(s => s.Timestamp);
            double[] timestamps = recording.Snapshots.Select(s => (s.Timestamp - start).TotalSeconds).ToArray();
            double[] throughput = recording.Snapshots.Select(s => s.CurrentThroughput).ToArray();

            plot.AddScatter(timestamps, throughput, Color.Blue, this._lineWidth, 5, MarkerShape.none, LineStyle.Solid, "Throughput (keys/sec)");

            plot.Title("Throughput");
            plot.Legend(false, Alignment.UpperLeft);
            plot.XAxis.Label("Time (sec)");
            plot.YAxis.Label("Keys");

            this.SaveImage(plot, directory, "throughput");
        }

        protected virtual void PlotProgress(EngineDataStoreBenchmarkResult recording, string directory)
        {
            Assert.ArgumentIsNotNull(directory, nameof(directory));
            Assert.ArgumentIsNotNull(recording, nameof(recording));

            Plot plot = new Plot(this._plotWith, this._plotHeight);

            DateTime start = recording.Snapshots.Min(s => s.Timestamp);
            double[] timestamps = recording.Snapshots.Select(s => (s.Timestamp - start).TotalSeconds).ToArray();
            double[] processed = recording.Snapshots.Select(s => (double) (s.Progress.Completed ?? 0L)).ToArray();
            double[] percentage = recording.Snapshots.Select(s => s.Progress.Ratio ?? 0.0).ToArray();

            double processedMinimum = processed.Min();
            double processedMaximum = processed.Max();
            double percentageMinimum = percentage.Min();
            double percentageMaximum = percentage.Max();

            ScatterPlot scatterProcessed = plot.AddScatter(timestamps, processed, Color.Blue, this._lineWidth, 5, MarkerShape.none, LineStyle.Solid, "Keys");
            ScatterPlot scatterPercentage = plot.AddScatter(timestamps, percentage, Color.Transparent, this._lineWidth, 5, MarkerShape.none, LineStyle.Solid, "Ratio");

            scatterProcessed.YAxisIndex= 0;
            scatterPercentage.YAxisIndex = 1;

            plot.Title("Progress");
            plot.Legend(false, Alignment.UpperLeft);

            plot.XAxis.Label("Time (sec)");
            plot.YAxis.Label("Keys");
            plot.YAxis.TickLabelFormat(value => this.FormatTickLabel(value, "N0", processedMinimum, processedMaximum));
            plot.YAxis2.Hide(false);
            plot.YAxis2.TickLabelFormat(value => this.FormatTickLabel(value, "P0", percentageMinimum, percentageMaximum));

            this.SaveImage(plot, directory, "progress");
        }

        protected string FormatTickLabel(double value, string format, double minimum, double maximum)
        {
            string result = string.Empty;

            if ((value >= minimum) && (value <= maximum))
            {
                result = value.ToString(format, CultureInfo.InvariantCulture);
            }

            return result;
        }

        protected void SaveImage(Plot plot, string directory, string name)
        {
            Assert.ArgumentIsNotNull(plot, nameof(plot));
            Assert.ArgumentIsNotNull(directory, nameof(directory));
            Assert.ArgumentIsNotNull(name, nameof(name));

            string path = Path.Combine(directory, name + ".png");

            plot.SaveFig(path);
        }
    }
}
