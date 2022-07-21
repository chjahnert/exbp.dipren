
using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    internal class DefaultEngineIdentityProviderTests
    {
        [Test]
        public void GetEngineIdentifier_InstanceCreated_ReturnsExpecedIdentifier()
        {
            IEngineIdentityProvider provider = new DefaultEngineIdentityProvider();

            string actual = provider.GetEngineIdentifier();

            string expected = FormattableString.Invariant($"{Environment.MachineName}{DefaultEngineIdentityProvider.DELIMITER}{Environment.ProcessId}{DefaultEngineIdentityProvider.DELIMITER}{AppDomain.CurrentDomain.Id}{DefaultEngineIdentityProvider.DELIMITER}");

            StringAssert.StartsWith(expected, actual);
        }
    }
}
