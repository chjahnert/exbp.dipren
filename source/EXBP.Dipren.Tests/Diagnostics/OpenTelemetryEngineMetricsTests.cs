
using EXBP.Dipren.Diagnostics;

using NUnit.Framework;

using OpenTelemetry;
using OpenTelemetry.Metrics;

using Assert = NUnit.Framework.Assert;


namespace EXBP.Dipren.Tests.Diagnostics
{
    [TestFixture]
    public class OpenTelemetryEngineMetricsTests
    {
        [Test]
        public void RegisterEngineState_MultipleChangesRegistered_InstrumentsReflectEvents()
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

            MetricSnapshot snapshot = metrics.LastOrDefault(m => m.Name == "engines" && m.MetricType == MetricType.LongGauge);

            Assert.That(snapshot, Is.Not.Null);
            Assert.That(snapshot.MetricPoints.Count, Is.EqualTo(4));

            MetricPoint p1 = snapshot.MetricPoints.FirstOrDefault(p => p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, "j1") && p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_STATE, OpenTelemetryEngineMetrics.TAG_VALUE_READY));
            MetricPoint p2 = snapshot.MetricPoints.FirstOrDefault(p => p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, "j1") && p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_STATE, OpenTelemetryEngineMetrics.TAG_VALUE_PROCESSING));
            MetricPoint p3 = snapshot.MetricPoints.FirstOrDefault(p => p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, "j2") && p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_STATE, OpenTelemetryEngineMetrics.TAG_VALUE_READY));
            MetricPoint p4 = snapshot.MetricPoints.FirstOrDefault(p => p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, "j2") && p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_STATE, OpenTelemetryEngineMetrics.TAG_VALUE_PROCESSING));

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
        public void RegisterPartitionCreated_MultipleEventsRegistered_InstrumentsReflectEvents()
        {
            List<MetricSnapshot> metrics = new List<MetricSnapshot>();

            using MeterProvider provider = Sdk.CreateMeterProviderBuilder()
                .AddDiprenMeters()
                .AddInMemoryExporter(metrics)
                .Build();

            string nodeId = "n1";
            string jobId = "j1";
            Guid partitionId = Guid.NewGuid();

            OpenTelemetryEngineMetrics.Instance.RegisterPartitionCreated(nodeId, jobId, partitionId);
            OpenTelemetryEngineMetrics.Instance.RegisterPartitionCreated(nodeId, jobId, partitionId, 2L);
            OpenTelemetryEngineMetrics.Instance.RegisterPartitionCreated(nodeId, jobId, partitionId, 5L);

            provider.Shutdown();

            MetricSnapshot snapshot = metrics.LastOrDefault(m => m.Name == OpenTelemetryEngineMetrics.INSTRUMENT_NAME_PARTITIONS_CREATED && m.MetricType == MetricType.LongSum);

            Assert.That(snapshot, Is.Not.Null);
            Assert.That(snapshot.MetricPoints.Count, Is.EqualTo(1));

            MetricPoint point = snapshot.MetricPoints.FirstOrDefault(p =>
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")));

            long count = point.GetSumLong();

            Assert.That(count, Is.EqualTo(8L));
        }

        [Test]
        public void RegisterPartitionCompleted_MultipleEventsRegistered_InstrumentsReflectEvents()
        {
            List<MetricSnapshot> metrics = new List<MetricSnapshot>();

            using MeterProvider provider = Sdk.CreateMeterProviderBuilder()
                .AddDiprenMeters()
                .AddInMemoryExporter(metrics)
                .Build();

            string nodeId = "n1";
            string jobId = "j1";
            Guid partitionId = Guid.NewGuid();

            OpenTelemetryEngineMetrics.Instance.RegisterPartitionCompleted(nodeId, jobId, partitionId);
            OpenTelemetryEngineMetrics.Instance.RegisterPartitionCompleted(nodeId, jobId, partitionId, 2L);
            OpenTelemetryEngineMetrics.Instance.RegisterPartitionCompleted(nodeId, jobId, partitionId, 5L);

            provider.Shutdown();

            MetricSnapshot snapshot = metrics.LastOrDefault(m => m.Name == OpenTelemetryEngineMetrics.INSTRUMENT_NAME_PARTITIONS_COMPLETED && m.MetricType == MetricType.LongSum);

            Assert.That(snapshot, Is.Not.Null);
            Assert.That(snapshot.MetricPoints.Count, Is.EqualTo(1));

            MetricPoint point = snapshot.MetricPoints.FirstOrDefault(p =>
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")));

            long count = point.GetSumLong();

            Assert.That(count, Is.EqualTo(8L));
        }

        [Test]
        public void RegisterBatchRetrieved_MultipleEventsRegistered_InstrumentsReflectEvents()
        {
            List<MetricSnapshot> metrics = new List<MetricSnapshot>();

            using MeterProvider provider = Sdk.CreateMeterProviderBuilder()
                .AddDiprenMeters()
                .AddInMemoryExporter(metrics)
                .Build();

            string nodeId = "n1";
            string jobId = "j1";
            Guid partitionId = Guid.NewGuid();

            OpenTelemetryEngineMetrics.Instance.RegisterBatchRetrieved(nodeId, jobId, partitionId, 7L, true, TimeSpan.FromMilliseconds(2));
            OpenTelemetryEngineMetrics.Instance.RegisterBatchRetrieved(nodeId, jobId, partitionId, 5L, true, TimeSpan.FromMilliseconds(3));
            OpenTelemetryEngineMetrics.Instance.RegisterBatchRetrieved(nodeId, jobId, partitionId, 3L, false, TimeSpan.FromMilliseconds(4));

            provider.Shutdown();

            {
                MetricSnapshot snapshot = metrics.LastOrDefault(m => m.Name == OpenTelemetryEngineMetrics.INSTRUMENT_NAME_KEYS_RETRIEVED && m.MetricType == MetricType.LongSum);

                Assert.That(snapshot, Is.Not.Null);
                Assert.That(snapshot.MetricPoints.Count, Is.EqualTo(2));

                MetricPoint pointSucceeded = snapshot.MetricPoints.FirstOrDefault(p =>
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_SUCCEEDED));

                long succeeded = pointSucceeded.GetSumLong();

                Assert.That(succeeded, Is.EqualTo(12L));

                MetricPoint pointFailed = snapshot.MetricPoints.FirstOrDefault(p =>
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_FAILED));

                long failed = pointFailed.GetSumLong();

                Assert.That(failed, Is.EqualTo(3L));
            }

            {
                MetricSnapshot snapshot = metrics.LastOrDefault(m => m.Name == OpenTelemetryEngineMetrics.INSTRUMENT_NAME_BATCHES_RETRIEVED && m.MetricType == MetricType.LongSum);

                Assert.That(snapshot, Is.Not.Null);
                Assert.That(snapshot.MetricPoints.Count, Is.EqualTo(2));

                MetricPoint pointSucceeded = snapshot.MetricPoints.FirstOrDefault(p =>
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_SUCCEEDED));

                long succeeded = pointSucceeded.GetSumLong();

                Assert.That(succeeded, Is.EqualTo(2L));

                MetricPoint pointFailed = snapshot.MetricPoints.FirstOrDefault(p =>
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_FAILED));

                long failed = pointFailed.GetSumLong();

                Assert.That(failed, Is.EqualTo(1L));
            }

            {
                MetricSnapshot snapshot = metrics.LastOrDefault(m => m.Name == OpenTelemetryEngineMetrics.INSTRUMENT_NAME_BATCH_RETRIEVAL && m.MetricType == MetricType.Histogram);

                Assert.That(snapshot, Is.Not.Null);
                Assert.That(snapshot.MetricPoints.Count, Is.EqualTo(2));

                MetricPoint pointSucceeded = snapshot.MetricPoints.FirstOrDefault(p =>
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_SUCCEEDED));

                long succeededCount = pointSucceeded.GetHistogramCount();
                double succeededSum = pointSucceeded.GetHistogramSum();

                Assert.That(succeededCount, Is.EqualTo(2));
                Assert.That(succeededSum, Is.EqualTo(5.0));

                MetricPoint pointFailed = snapshot.MetricPoints.FirstOrDefault(p =>
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_FAILED));

                long failedCount = pointFailed.GetHistogramCount();
                double failedSum = pointFailed.GetHistogramSum();

                Assert.That(failedCount, Is.EqualTo(1));
                Assert.That(failedSum, Is.EqualTo(4.0));
            }
        }

        [Test]
        public void RegisterBatchProcessed_MultipleEventsRegistered_InstrumentsReflectEvents()
        {
            List<MetricSnapshot> metrics = new List<MetricSnapshot>();

            using MeterProvider provider = Sdk.CreateMeterProviderBuilder()
                .AddDiprenMeters()
                .AddInMemoryExporter(metrics)
                .Build();

            string nodeId = "n1";
            string jobId = "j1";
            Guid partitionId = Guid.NewGuid();

            OpenTelemetryEngineMetrics.Instance.RegisterBatchProcessed(nodeId, jobId, partitionId, 7L, true, TimeSpan.FromMilliseconds(2));
            OpenTelemetryEngineMetrics.Instance.RegisterBatchProcessed(nodeId, jobId, partitionId, 5L, true, TimeSpan.FromMilliseconds(3));
            OpenTelemetryEngineMetrics.Instance.RegisterBatchProcessed(nodeId, jobId, partitionId, 3L, false, TimeSpan.FromMilliseconds(4));

            provider.Shutdown();

            {
                MetricSnapshot snapshot = metrics.LastOrDefault(m => m.Name == OpenTelemetryEngineMetrics.INSTRUMENT_NAME_KEYS_COMPLETED && m.MetricType == MetricType.LongSum);

                Assert.That(snapshot, Is.Not.Null);
                Assert.That(snapshot.MetricPoints.Count, Is.EqualTo(2));

                MetricPoint pointSucceeded = snapshot.MetricPoints.FirstOrDefault(p =>
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_SUCCEEDED));

                long succeeded = pointSucceeded.GetSumLong();

                Assert.That(succeeded, Is.EqualTo(12L));

                MetricPoint pointFailed = snapshot.MetricPoints.FirstOrDefault(p =>
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_FAILED));

                long failed = pointFailed.GetSumLong();

                Assert.That(failed, Is.EqualTo(3L));
            }

            {
                MetricSnapshot snapshot = metrics.LastOrDefault(m => m.Name == OpenTelemetryEngineMetrics.INSTRUMENT_NAME_BATCHES_COMPLETED && m.MetricType == MetricType.LongSum);

                Assert.That(snapshot, Is.Not.Null);
                Assert.That(snapshot.MetricPoints.Count, Is.EqualTo(2));

                MetricPoint pointSucceeded = snapshot.MetricPoints.FirstOrDefault(p =>
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_SUCCEEDED));

                long succeeded = pointSucceeded.GetSumLong();

                Assert.That(succeeded, Is.EqualTo(2L));

                MetricPoint pointFailed = snapshot.MetricPoints.FirstOrDefault(p =>
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_FAILED));

                long failed = pointFailed.GetSumLong();

                Assert.That(failed, Is.EqualTo(1L));
            }

            {
                MetricSnapshot snapshot = metrics.LastOrDefault(m => m.Name == OpenTelemetryEngineMetrics.INSTRUMENT_NAME_BATCH_PROCESSING && m.MetricType == MetricType.Histogram);

                Assert.That(snapshot, Is.Not.Null);
                Assert.That(snapshot.MetricPoints.Count, Is.EqualTo(2));

                MetricPoint pointSucceeded = snapshot.MetricPoints.FirstOrDefault(p =>
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_SUCCEEDED));

                long succeededCount = pointSucceeded.GetHistogramCount();
                double succeededSum = pointSucceeded.GetHistogramSum();

                Assert.That(succeededCount, Is.EqualTo(2));
                Assert.That(succeededSum, Is.EqualTo(5.0));

                MetricPoint pointFailed = snapshot.MetricPoints.FirstOrDefault(p =>
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_PARTITION, partitionId.ToString("d")) &&
                    p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_FAILED));

                long failedCount = pointFailed.GetHistogramCount();
                double failedSum = pointFailed.GetHistogramSum();

                Assert.That(failedCount, Is.EqualTo(1));
                Assert.That(failedSum, Is.EqualTo(4.0));
            }
        }

        [Test]
        public void RegisterReportProgress_MultipleEventsRegistered_InstrumentsReflectEvents()
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

            MetricSnapshot snapshot = metrics.LastOrDefault(m => m.Name == OpenTelemetryEngineMetrics.INSTRUMENT_NAME_REPORT_PROGRESS && m.MetricType == MetricType.Histogram);

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

        [Test]
        public void RegisterIsSplitRequestPending_MultipleEventsRegistered_InstrumentsReflectEvents()
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

            OpenTelemetryEngineMetrics.Instance.RegisterIsSplitRequestPending(nodeId, jobId, duration1);
            OpenTelemetryEngineMetrics.Instance.RegisterIsSplitRequestPending(nodeId, jobId, duration2);

            provider.Shutdown();

            MetricSnapshot snapshot = metrics.LastOrDefault(m => m.Name == OpenTelemetryEngineMetrics.INSTRUMENT_NAME_IS_SPLIT_REQUEST_PENDING && m.MetricType == MetricType.Histogram);

            Assert.That(snapshot, Is.Not.Null);
            Assert.That(snapshot.MetricPoints.Count, Is.EqualTo(1));

            MetricPoint point = snapshot.MetricPoints.FirstOrDefault(p =>
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId));

            long count = point.GetHistogramCount();
            double sum = point.GetHistogramSum();

            Assert.That(count, Is.EqualTo(2));
            Assert.That(sum, Is.EqualTo(5));
        }

        [Test]
        public void RegisterTryAcquirePartition_MultipleEventsRegistered_InstrumentsReflectEvents()
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
            TimeSpan duration3 = TimeSpan.FromMilliseconds(7);

            OpenTelemetryEngineMetrics.Instance.RegisterTryAcquirePartition(nodeId, jobId, true, duration1);
            OpenTelemetryEngineMetrics.Instance.RegisterTryAcquirePartition(nodeId, jobId, true, duration2);
            OpenTelemetryEngineMetrics.Instance.RegisterTryAcquirePartition(nodeId, jobId, false, duration3);

            provider.Shutdown();

            MetricSnapshot snapshot = metrics.LastOrDefault(m => m.Name == OpenTelemetryEngineMetrics.INSTRUMENT_NAME_TRY_ACQUIRE_PARTITION && m.MetricType == MetricType.Histogram);

            Assert.That(snapshot, Is.Not.Null);
            Assert.That(snapshot.MetricPoints.Count, Is.EqualTo(2));

            MetricPoint pointSuccess = snapshot.MetricPoints.FirstOrDefault(p =>
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_SUCCEEDED));

            long countSuccess = pointSuccess.GetHistogramCount();
            double sumSuccess = pointSuccess.GetHistogramSum();

            Assert.That(countSuccess, Is.EqualTo(2));
            Assert.That(sumSuccess, Is.EqualTo(5));

            MetricPoint pointFailure = snapshot.MetricPoints.FirstOrDefault(p =>
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_FAILED));

            long countFailure = pointFailure.GetHistogramCount();
            double sumFailure = pointFailure.GetHistogramSum();

            Assert.That(countFailure, Is.EqualTo(1));
            Assert.That(sumFailure, Is.EqualTo(7));
        }

        [Test]
        public void RegisterTryRequestSplit_MultipleEventsRegistered_InstrumentsReflectEvents()
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
            TimeSpan duration3 = TimeSpan.FromMilliseconds(7);

            OpenTelemetryEngineMetrics.Instance.RegisterTryRequestSplit(nodeId, jobId, true, duration1);
            OpenTelemetryEngineMetrics.Instance.RegisterTryRequestSplit(nodeId, jobId, true, duration2);
            OpenTelemetryEngineMetrics.Instance.RegisterTryRequestSplit(nodeId, jobId, false, duration3);

            provider.Shutdown();

            MetricSnapshot snapshot = metrics.LastOrDefault(m => m.Name == OpenTelemetryEngineMetrics.INSTRUMENT_NAME_TRY_REQUEST_SPLIT && m.MetricType == MetricType.Histogram);

            Assert.That(snapshot, Is.Not.Null);
            Assert.That(snapshot.MetricPoints.Count, Is.EqualTo(2));

            MetricPoint pointSuccess = snapshot.MetricPoints.FirstOrDefault(p =>
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_SUCCEEDED));

            long countSuccess = pointSuccess.GetHistogramCount();
            double sumSuccess = pointSuccess.GetHistogramSum();

            Assert.That(countSuccess, Is.EqualTo(2));
            Assert.That(sumSuccess, Is.EqualTo(5));

            MetricPoint pointFailure = snapshot.MetricPoints.FirstOrDefault(p =>
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_NODE, nodeId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_JOB, jobId) &&
                p.HasTag(OpenTelemetryEngineMetrics.TAG_NAME_OUTCOME, OpenTelemetryEngineMetrics.TAG_VALUE_FAILED));

            long countFailure = pointFailure.GetHistogramCount();
            double sumFailure = pointFailure.GetHistogramSum();

            Assert.That(countFailure, Is.EqualTo(1));
            Assert.That(sumFailure, Is.EqualTo(7));
        }
    }
}
