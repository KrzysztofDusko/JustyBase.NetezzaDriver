using JustyBase.NetezzaDriver;
using System.Diagnostics;
using System.Globalization;
using System.Text;

internal static class NzConnectionHelpers
{
    public static readonly Encoding ClientEncoding = Encoding.UTF8;
    public static readonly Encoding CharVarcharEncoding = Encoding.Latin1;
    public const string NZPY_CLIENT_VERSION = "Release 11.1.0.0";

    //add pooling ? 
    public static string TextRecv(byte[] data, int offset, int length)
    {
        return ClientEncoding.GetString(data, offset, length);
    }

    /// <summary>
    /// SELECT * FROM SYSTEM.ADMIN._T_PROC ORDER BY ROWID LIMIT 1000
    /// </summary>
    /// <param name="data"></param>
    /// <param name="offset"></param>
    /// <param name="length"></param>
    /// <returns></returns>
    public static string ByteaRecv(byte[] data, int offset, int length)
    {
        //byte[] result = new byte[length];
        //Array.Copy(data, offset, result, 0, length);
        //return result;
        return Encoding.ASCII.GetString(data, offset, length);
    }

    public static bool BoolRecvTyped(byte[] data, int offset, int length)
    {
        return data[offset] == 116; // ASCII for 't'= (byte)'t', (byte)'f' = 102
    }

    //change to sbyte ?, OID 2500
    public static Int16 ByteRecvTyped(byte[] data, int offset, int length)
    {
        //Int16 to be in pair with ODBC
        return (Int16)SByte.Parse(data.AsSpan().Slice(offset, length));
    }

    public static long Int8RecvTyped(byte[] data, int offset, int length)
    {
        return long.Parse(data.AsSpan().Slice(offset, length));
    }

    public static Int16 Int2RecvTyped(byte[] data, int offset, int length)
    {
        return Int16.Parse(data.AsSpan().Slice(offset, length));
    }

    public static int Int4RecvTyped(byte[] data, int offset, int length)
    {
        return Int32.Parse(data.AsSpan().Slice(offset, length));
    }

    public static float Float4RecvTyped(byte[] data, int offset, int length)
    {
        return float.Parse(data.AsSpan().Slice(offset, length), CultureInfo.InvariantCulture);
    }

    public static double Float8RecvTyped(byte[] data, int offset, int length)
    {
        return double.Parse(data.AsSpan().Slice(offset, length), CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// occurs for SELECT '2024-12-12'::DATE
    /// </summary>
    /// <param name="data"></param>
    /// <param name="offset"></param>
    /// <param name="length"></param>
    /// <returns></returns>
    public static DateTime DateInTyped(byte[] data, int offset, int length)
    {
        //string dateStr = _clientEncoding.GetString(data, offset, length);
        ReadOnlySpan<byte> spB = new ReadOnlySpan<byte>(data, offset, length);
        try
        {
            return new DateTime(
                int.Parse(spB[0..4]),  // year
                int.Parse(spB[5..7]),  // month
                int.Parse(spB[8..10])); // day
        }
        catch (Exception)
        {
            return DateTime.MinValue;
        }
    }

    /// <summary>
    /// to do eliminate allocation
    /// </summary>
    /// <param name="data"></param>
    /// <param name="offset"></param>
    /// <param name="length"></param>
    /// <returns></returns>
    public static DateTime TimestamptzRecvFloatTyped(byte[] data, int offset, int length)
    {
        Span<char> chars = length < 128 ? stackalloc char[length] : new char[length];
        int charCnt = Encoding.ASCII.GetChars(data.AsSpan().Slice(offset, length), chars);
        chars = chars[..charCnt];
        //var data1 = _clientEncoding.GetString(data, offset, length);

        if (DateTime.TryParse(chars, out var res))
        {
            return res;
        }
        else
        {
            //return new string(chars);
            return DateTime.MinValue;
        }
    }

    public static TimeSpan TimeInTyped(byte[] data, int offset, int length)
    {
        int hour = int.Parse(data.AsSpan().Slice(offset, 2));
        int minute = int.Parse(data.AsSpan().Slice(offset + 3, 2));
        decimal seconds = decimal.Parse(data.AsSpan().Slice(offset + 6, length - 6), CultureInfo.InvariantCulture);

        return new TimeSpan(
            0,
            hour,
            minute,
            (int)seconds,
            (int)((seconds - Math.Floor(seconds)) * 1000000));
    }

    private const double EPOCH_SECONDS = 946684800.0; // 2000-01-01 UTC


    //private static object TimestampRecvFloat(byte[] data, int offset, int length)
    //{
    //    double seconds = BitConverter.ToDouble(data, offset);
    //    return DateTime.UnixEpoch.AddSeconds(EPOCH_SECONDS + seconds);
    //}

    public static DateTime TimestampRecvFloatTyped(byte[] data, int offset, int length)
    {
        double seconds = BitConverter.ToDouble(data, offset);
        return DateTime.UnixEpoch.AddSeconds(EPOCH_SECONDS + seconds);
    }

    /// <summary>
    /// Converts PostgreSQL numeric format to C# decimal
    /// occurs for SELECT 3.14::NUMERIC(10,4)
    /// </summary>
    public static decimal NumericInTyped(byte[] data, int offset, int length)
    {
        var sp = new ReadOnlySpan<byte>(data, offset, length);
        return decimal.Parse(sp, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Receives timestamp with timezone as integer from PostgreSQL data stream
    /// </summary>
    //private object TimestamptzRecvInteger(byte[] data, int offset, int length)
    //{
    //    Debug.Fail("TimestamptzRecvInteger usage");
    //    //
    //    //return Encoding.UTF8.GetString(data, offset, length);
    //    return Encoding.ASCII.GetString(data, offset, length);
    //}

    /// <summary>
    /// Converts C# decimal to PostgreSQL numeric format
    /// </summary>
    //private byte[] NumericOut(decimal d)
    //{
    //    Debug.Assert(false);
    //    //Encoding.ASCII ?
    //    return _clientEncoding.GetBytes(d.ToString(CultureInfo.InvariantCulture));
    //}

    /// <summary>
    /// Receives interval as integer from PostgreSQL data stream
    /// SELECT '5 hours 41 minutes  15 sec'::interval
    /// </summary>
    public static string IntervalRecvInteger(byte[] data, int offset, int length)
    {
        return Encoding.ASCII.GetString(data, offset, length);
    }

    /// <summary>
    /// Receives UUID from data stream
    /// TODO 
    /// </summary>
    public static Guid UuidRecvTyped(byte[] data, int offset, int length)
    {
        Debug.Assert(false);
        if (length != 16)
        {
            throw new ArgumentException("UUID must be exactly 16 bytes");
        }
        return new Guid(data.AsSpan().Slice(offset, length));
    }

}
