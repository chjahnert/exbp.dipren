
using OpenTelemetry.Metrics;


namespace EXBP.Dipren.Tests.Telemetry
{
    internal static class MetricPointExtensions
    {
        internal static bool HasTag(this MetricPoint point, string name, object value)
        {
            bool result = false;

            foreach (KeyValuePair<string, object> tag in point.Tags)
            {
                result = (name.Equals(tag.Key) && value.Equals(tag.Value));

                if (result == true)
                {
                    break;
                }
            }

            return result;
        }
    }
}
