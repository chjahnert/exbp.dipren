
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class JobNotScheduledExceptionTests
    {
        [Test]
        public void Serialization_ValidInstance_IsRestored()
        {
            const string id = "DPJ-0001";

            JobNotScheduledException exception = new JobNotScheduledException("Message", id);
            JobNotScheduledException restored = SerializationUtilities.Serialize(exception);

            Assert.That(restored, Is.Not.Null);
            Assert.That(restored.Id, Is.EqualTo(id));
        }
    }
}
