
using OpenTelemetry.Metrics;


namespace EXBP.Dipren.Diagnostics
{
    /// <summary>
    ///   Implements extension methods for the <see cref="MeterProviderBuilder"/> class.
    /// </summary>
    public static class MeterProviderBuilderExtensions
    {
        /// <summary>
        ///   Adds the Dipren meter to the <see cref="MeterProviderBuilder"/> instance.
        /// </summary>
        /// <param name="builder">
        ///   The <see cref="MeterProviderBuilder"/> to which to add the Dipren meters.
        /// </param>
        /// <returns>
        ///   The <see cref="MeterProviderBuilder"/> for chaining.
        /// </returns>
        public static MeterProviderBuilder AddDiprenMeters(this MeterProviderBuilder builder)
        {
            Assert.ArgumentIsNotNull(builder, nameof(builder));

            builder.AddMeter(OpenTelemetryEngineMetrics.MeterName);

            return builder;
        }
    }
}
