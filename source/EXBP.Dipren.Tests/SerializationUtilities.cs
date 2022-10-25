
#pragma warning disable SYSLIB0011

using System.Diagnostics;
using System.Runtime.Serialization.Formatters.Binary;


namespace EXBP.Dipren.Tests
{
    internal static class SerializationUtilities
    {
        internal static T Serialize<T>(T instance)
        {
            Debug.Assert(instance != null);

            BinaryFormatter formatter = new BinaryFormatter();

            T result = default;

            using (MemoryStream stream = new MemoryStream())
            {
                formatter.Serialize(stream, instance);

                stream.Seek(0, 0);

                result = (T) formatter.Deserialize(stream);
            }

            return result;
        }
    }
}
