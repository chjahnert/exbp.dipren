
using System.Globalization;
using System.Numerics;

using EXBP.Dipren.Data;

using NUnit.Framework;


namespace EXBP.Dipren.Tests.Data
{
    [TestFixture]
    public class BigIntegerKeySerializerTests
    {
        [Test]
        public void Serialize_ArgumentValueIsValid_SerializesValueCorrectly()
        {
            const string sv = "367523562837956287356287356289346598576295789345793456792356797347659";

            BigInteger value = BigInteger.Parse(sv, CultureInfo.InvariantCulture);

            BigIntegerKeySerializer serializer = new BigIntegerKeySerializer();
            string serialized = serializer.Serialize(value);

            Assert.That(serialized, Is.EqualTo(sv));
        }

        [Test]
        public void Deserialize_ArgumentValueIsValid_DeserializesValueCorrectly()
        {
            const string sv = "367523562837956287356287356289346598576295789345793456792356797347659";

            BigIntegerKeySerializer serializer = new BigIntegerKeySerializer();
            BigInteger deserialized = serializer.Deserialize(sv);

            BigInteger expected = BigInteger.Parse(sv, CultureInfo.InvariantCulture);

            Assert.That(deserialized, Is.EqualTo(expected));
        }
    }
}
