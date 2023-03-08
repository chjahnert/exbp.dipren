
namespace EXBP.Dipren
{
    /// <summary>
    ///   Calculates a moving average.
    /// </summary>
    internal class MovingAverage
    {
        private readonly double[] _values;
        private int _position = 0;
        private bool _saturated = false;


        /// <summary>
        ///   Gets the number of values contained in the current instance.
        /// </summary>
        /// <value>
        ///   A <see cref="int"/> value that contains the number of values contained in the current instance.
        /// </value>
        public int Count => (this._saturated == true) ? this._values.Length : this._position;


        /// <summary>
        ///   Gets the average of the contained values.
        /// </summary>
        /// <value>
        ///   A <see cref="double"/> value that contains the average value of the contained values.
        /// </value>
        public double Average
        {
            get
            {
                double result = 0D;

                if (this._saturated == true)
                {
                    result = this._values.Average();
                }
                else
                {
                    if (this._position > 0)
                    {
                        result = this._values.Take(this._position).Average();
                    }
                }

                return result;
            }
        }


        /// <summary>
        ///   Initializes a new instance of the <see cref="MovingAverage"/> class.
        /// </summary>
        /// <param name="size">
        ///   The number of values to use to calculate the average.
        /// </param>
        public MovingAverage(int size)
        {
            this._values = new double[size];
        }


        /// <summary>
        ///   Adds a new value to the set of values.
        /// </summary>
        /// <param name="value">
        ///   The value to add.
        /// </param>
        /// <remarks>
        ///   If the 
        /// </remarks>
        public void Add(double value)
        {
            this._values[this._position] = value;

            this._position += 1;

            if (this._position >= this._values.Length)
            {
                this._position = 0;
                this._saturated = true;
            }
        }
    }
}
