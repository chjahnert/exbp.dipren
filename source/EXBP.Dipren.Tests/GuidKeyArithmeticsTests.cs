
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;


namespace EXBP.Dipren.Tests
{
    [TestFixture]
    public class GuidKeyArithmeticsTests
    {
        [Test]
        public void Ctor_ArgumentLayoutIsNull_ThrowsException()
        {
            Assert.Throws<ArgumentNullException>(() => new GuidKeyArithmetics(null));
        }

        [TestCase(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14} )]
        [TestCase(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 14} )]
        [TestCase(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 33} )]
        public void Ctor_ArgumentLayoutIsNotValid_ThrowsException(byte[] layout)
        {
            Assert.Throws<ArgumentException>(() => new GuidKeyArithmetics(layout));
        }
    }
}
