
namespace EXBP.Dipren.Tests
{
    internal class PresetDateTimeProvider : IDateTimeProvider
    {
        private DateTime _value;


        public DateTimeKind Kind => this._value.Kind;


        public PresetDateTimeProvider(DateTime value)
        {
            this._value = value;
        }


        public DateTime GetDateTime() => this._value;

        public void Add(double milliseconds)
        {
            TimeSpan timespan = TimeSpan.FromMilliseconds(milliseconds);

            this.Add(timespan);
        }

        public void Add(TimeSpan timespan)
        {
            this._value += timespan;
        }
    }
}
