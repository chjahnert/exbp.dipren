
using EXBP.Dipren.Telemetry;

using NUnit.Framework;

using OpenTelemetry;
using OpenTelemetry.Metrics;


namespace EXBP.Dipren.Tests.Telemetry
{
    [TestFixture]
    public class OpenTelemetryEngineMetricsTests
    {
        [Test]
        public void RegisterEngineState_ValidArguments_UpdatesCounter()
        {
            List<MetricSnapshot> metrics = new List<MetricSnapshot>();

            using MeterProvider provider = Sdk.CreateMeterProviderBuilder()
                .AddDiprenMeters()
                .AddInMemoryExporter(metrics)
                .Build();

            OpenTelemetryEngineMetrics.Instance.RegisterEngineState("n1", "j1", EngineState.Ready);
            OpenTelemetryEngineMetrics.Instance.RegisterEngineState("n2", "j2", EngineState.Ready);
            OpenTelemetryEngineMetrics.Instance.RegisterEngineState("n3", "j2", EngineState.Ready);

            provider.Shutdown();

            MetricSnapshot snapshot = metrics.LastOrDefault(m => (m.Name == "engines") && (m.MetricType == MetricType.LongGauge));

            Assert.That(snapshot, Is.Not.Null);
            Assert.That(snapshot.MetricPoints.Count, Is.EqualTo(4));

            MetricPoint p1 = snapshot.MetricPoints.FirstOrDefault(p => p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, "j1") && (p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_STATE, OpenTelemetryEngineMetrics.TAG_VALUE_READY)));
            MetricPoint p2 = snapshot.MetricPoints.FirstOrDefault(p => p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, "j1") && (p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_STATE, OpenTelemetryEngineMetrics.TAG_VALUE_PROCESSING)));
            MetricPoint p3 = snapshot.MetricPoints.FirstOrDefault(p => p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, "j2") && (p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_STATE, OpenTelemetryEngineMetrics.TAG_VALUE_READY)));
            MetricPoint p4 = snapshot.MetricPoints.FirstOrDefault(p => p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, "j2") && (p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_STATE, OpenTelemetryEngineMetrics.TAG_VALUE_PROCESSING)));

            long c1 = p1.GetGaugeLastValueLong();
            long c2 = p2.GetGaugeLastValueLong();
            long c3 = p3.GetGaugeLastValueLong();
            long c4 = p4.GetGaugeLastValueLong();

            Assert.That(c1, Is.EqualTo(1L));
            Assert.That(c2, Is.EqualTo(0L));
            Assert.That(c3, Is.EqualTo(2L));
            Assert.That(c4, Is.EqualTo(0L));
        }

        [Test]
        public void RegisterReportProgress_MultipleEventsRegistered_InstrumentReflectsEvents()
        {
            List<MetricSnapshot> metrics = new List<MetricSnapshot>();

            using MeterProvider provider = Sdk.CreateMeterProviderBuilder()
                .AddDiprenMeters()
                .AddInMemoryExporter(metrics)
                .Build();

            string nodeId = "n1";
            string jobId = "j1";
            Guid partitionId = Guid.NewGuid();
            TimeSpan duration1 = TimeSpan.FromMilliseconds(2);
            TimeSpan duration2 = TimeSpan.FromMilliseconds(3);

            OpenTelemetryEngineMetrics.Instance.RegisterReportProgress(nodeId, jobId, partitionId, duration1);
            OpenTelemetryEngineMetrics.Instance.RegisterReportProgress(nodeId, jobId, partitionId, duration2);

            provider.Shutdown();

            MetricSnapshot snapshot = metrics.LastOrDefault(m => (m.Name == OpenTelemetryEngineMetrics.INSTRUMENT_NAME_REPORT_PROGRESS) && (m.MetricType == MetricType.Histogram));

            Assert.That(snapshot, Is.Not.Null);
            Assert.That(snapshot.MetricPoints.Count, Is.EqualTo(1));

            MetricPoint point = snapshot.MetricPoints.FirstOrDefault(p =>
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")));

            long count = point.GetHistogramCount();
            double sum = point.GetHistogramSum();

            Assert.That(count, Is.EqualTo(2));
            Assert.That(sum, Is.EqualTo(5));
        }
    }
}
