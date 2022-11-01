﻿
using EXBP.Dipren.Diagnostics;


namespace EXBP.Dipren
{
    /// <summary>
    ///   Implements key range arithmetics for the <see cref="Int32"/> based key ranges.
    /// </summary>
    public class Int32KeyArithmetics : IKeyArithmetics<int>
    {
        private readonly IComparer<int> _comparer;


        /// <summary>
        ///   Gets the default instance of the <see cref="Int32KeyArithmetics"/> class.
        /// </summary>
        /// <value>
        ///   An <see cref="Int32KeyArithmetics"/> object that uses the default comparer for the <see cref="int"/>
        ///   type.
        /// </value>
        public static Int32KeyArithmetics Default { get; } = new Int32KeyArithmetics(Comparer<int>.Default);


        /// <summary>
        ///   Initializes a new instance of the <see cref="Int32KeyArithmetics"/> class.
        /// </summary>
        /// <param name="comparer">
        ///   The <see cref="IComparable{T}"/> of <see cref="int"/> object to use to compare key values.
        /// </param>
        public Int32KeyArithmetics(IComparer<int> comparer)
        {
            Assert.ArgumentIsNotNull(comparer, nameof(comparer));

            this._comparer = comparer;
        }


        /// <summary>
        ///   Splits the specified range into two ranges.
        /// </summary>
        /// <param name="range">
        ///   The <see cref="Range{TKey}"/> of <see cref="Int32"/> values to split.
        /// </param>
        /// <param name="created">
        ///   A variable that receives the new <paramref name="range"/> object created.
        /// </param>
        /// <returns>
        ///   A <see cref="Range{TKey}"/> of <see cref="Int32"/> objects that is the updated value of
        ///   <paramref name="range"/>.
        /// </returns>
        public Range<int> Split(Range<int> range, out Range<int> created)
        {
            Assert.ArgumentIsNotNull(range, nameof(range));

            Range<int> result = range;
            created = null;

            double distance = Math.Abs(range.Last - range.First);

            if (distance >= 2)
            {
                int half = (int) Math.Round(distance / 2);

                bool ascending = range.IsAscending(this._comparer); 

                if (ascending == false)
                {
                    half *= -1;
                }

                result = new Range<int>(range.First, range.First + half, false);
                created = new Range<int>(range.First + half, range.Last, range.IsInclusive);
            }

            return result;
        }
    }
}
