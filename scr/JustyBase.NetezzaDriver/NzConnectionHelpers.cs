using JustyBase.NetezzaDriver;
using System.Diagnostics;
using System.Globalization;
using System.Text;

internal static class NzConnectionHelpers
{
    public static readonly Encoding ClientEncoding = Encoding.UTF8;
    public static readonly Encoding CharVarcharEncoding = Encoding.Latin1;
    public const string NZPY_CLIENT_VERSION = "Release 11.1.0.0";

    //add string pooling ? 
    private static object TextRecv(byte[] data, int offset, int length)
    {
        return ClientEncoding.GetString(data, offset, length);
    }
    private static object ByteaRecv(byte[] data, int offset, int length)
    {
        //byte[] result = new byte[length];
        //Array.Copy(data, offset, result, 0, length);
        //return result;
        return Encoding.ASCII.GetString(data, offset, length);
    }
    //boxing....
    private static object BoolRecv(byte[] data, int offset, int length)
    {
        return data[offset] == 116; // ASCII for 't'= (byte)'t', (byte)'f' = 102
    }

    //boxing....
    private static object ByteRecv(byte[] data, int offset, int length)
    {
        //Int16 to be in pair with ODBC
        return (Int16)SByte.Parse(data.AsSpan().Slice(offset, length));
    }

    //boxing....
    private static object Int8Recv(byte[] data, int offset, int length)
    {
        return long.Parse(data.AsSpan().Slice(offset, length));
    }
    //boxing....
    private static object Int2Recv(byte[] data, int offset, int length)
    {
        return Int16.Parse(data.AsSpan().Slice(offset, length));
    }
    //boxing....
    private static object Int4Recv(byte[] data, int offset, int length)
    {
        return Int32.Parse(data.AsSpan().Slice(offset, length));
    }
    //boxing....
    private static object IntIn(byte[] data, int offset, int length)
    {
        return Int4Recv(data, offset, length);
    }
    //boxing....
    private static object Float4Recv(byte[] data, int offset, int length)
    {
        return float.Parse(data.AsSpan().Slice(offset, length), CultureInfo.InvariantCulture);
    }
    //boxing....
    private static object Float8Recv(byte[] data, int offset, int length)
    {
        return double.Parse(data.AsSpan().Slice(offset, length), CultureInfo.InvariantCulture);
    }

    //boxing....
    private static object DateIn(byte[] data, int offset, int length)
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
    /// Receives timestamp with timezone as float from PostgreSQL data stream
    /// </summary>
    private static object TimestamptzRecvFloat(byte[] data, int offset, int length)
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
            return new string(chars);
        }
    }

    private static object TimeIn(byte[] data, int offset, int length)
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
    /// <summary>
    /// Receives timestamp as float from PostgreSQL data stream and converts to UTC DateTime
    /// </summary>
    private static object TimestampRecvFloat(byte[] data, int offset, int length)
    {
        double seconds = BitConverter.ToDouble(data, offset);
        return DateTime.UnixEpoch.AddSeconds(EPOCH_SECONDS + seconds);
    }

    /// <summary>
    /// Converts PostgreSQL numeric format to C# decimal
    /// </summary>
    private static object NumericIn(byte[] data, int offset, int length)
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
    /// add pooling ? 
    /// </summary>
    private static string IntervalRecvInteger(byte[] data, int offset, int length)
    {
        //return Encoding.UTF8.GetString(data, offset, length);
        return Encoding.ASCII.GetString(data, offset, length);
    }

    /// <summary>
    /// Receives UUID from PostgreSQL data stream
    /// </summary>
    private static object UuidRecv(byte[] data, int offset, int length)
    {
        Debug.Assert(false);
        if (length != 16)
        {
            throw new ArgumentException("UUID must be exactly 16 bytes");
        }
        return new Guid(data.AsSpan().Slice(offset, length));
    }

    private const int FC_TEXT = 0;
    private const int FC_BINARY = 1;


    private static readonly Dictionary<int, (int format, Func<byte[], int, int, object> receiver)> _pgTypes = new Dictionary<int, (int, Func<byte[], int, int, object>)>
        {
            { 16, (FC_BINARY, BoolRecv)},     // boolean
            { 17, (FC_BINARY, ByteaRecv) },    // bytea
            { 19, (FC_BINARY, TextRecv) },     // name type
            { 20, (FC_BINARY, Int8Recv) },     // int8
            { 21, (FC_BINARY, Int2Recv) },     // int2
            //TODO { 22, (FC_TEXT, VectorIn) },       // int2vector
            { 23, (FC_BINARY, Int4Recv) },     // int4
            { 25, (FC_BINARY, TextRecv) },     // TEXT type
            { 26, (FC_TEXT, IntIn) },          // oid
            { 28, (FC_TEXT, IntIn) },          // xid
            //TODO{ 114, (FC_TEXT, JsonIn) },        // json
            { 700, (FC_BINARY, Float4Recv) },  // float4
            { 701, (FC_BINARY, Float8Recv) },  // float8
            { 702, (FC_BINARY, TimestamptzRecvFloat) },  // SELECT CREATEDATE FROM _V_TABLE ORDER BY CREATEDATE DESC .. 
            { 705, (FC_BINARY, TextRecv) },    // unknown
            { 829, (FC_TEXT, TextRecv) },      // MACADDR type
            //TODO{ 1000, (FC_BINARY, ArrayRecv) },  // BOOL[]
            //TODO{ 1003, (FC_BINARY, ArrayRecv) },  // NAME[]
            //TODO{ 1005, (FC_BINARY, ArrayRecv) },  // INT2[]
            //TODO{ 1007, (FC_BINARY, ArrayRecv) },  // INT4[]
            //TODO{ 1009, (FC_BINARY, ArrayRecv) },  // TEXT[]
            //TODO{ 1014, (FC_BINARY, ArrayRecv) },  // CHAR[]
            //TODO{ 1015, (FC_BINARY, ArrayRecv) },  // VARCHAR[]
            //TODO{ 1016, (FC_BINARY, ArrayRecv) },  // INT8[]
            //TODO{ 1021, (FC_BINARY, ArrayRecv) },  // FLOAT4[]
            //TODO{ 1022, (FC_BINARY, ArrayRecv) },  // FLOAT8[]
            { 1042, (FC_BINARY, TextRecv) },   // CHAR type
            { 1043, (FC_BINARY, TextRecv) },   // VARCHAR type
            { 1082, (FC_TEXT, DateIn) },       // date
            { 1083, (FC_TEXT, TimeIn) },       // time
            { 1114, (FC_BINARY, TimestampRecvFloat) },    // timestamp w/ tz
            { 1184, (FC_BINARY, TimestamptzRecvFloat) },
            { 1186, (FC_BINARY, IntervalRecvInteger) },
            //TODO{ 1231, (FC_TEXT, ArrayIn) },      // NUMERIC[]
            //TODO{ 1263, (FC_BINARY, ArrayRecv) },  // cstring[]
            { 1700, (FC_TEXT, NumericIn) },    // NUMERIC
            { 2275, (FC_BINARY, TextRecv) },   // cstring
            { 2500, (FC_BINARY, ByteRecv) },   // SELECT 15::BYTEINT

            { 2950, (FC_BINARY, UuidRecv) },   // uuid
            //TODO{{ 3802, (FC_TEXT, JsonIn) }        // jsonb
    };

    public static (int format, Func<byte[], int, int, object> receiver) GetPgType(int oid)
    {
        return _pgTypes.TryGetValue(oid, out var type) ? type : (FC_TEXT, TextRecv);
    }


    public static readonly Dictionary<int, string> DataType = new()
    {
        { NzConnection.NzTypeChar, "NzTypeChar" },
        { NzConnection.NzTypeVarChar, "NzTypeVarChar" },
        { NzConnection.NzTypeVarFixedChar, "NzTypeVarFixedChar" },
        { NzConnection.NzTypeGeometry, "NzTypeGeometry" },
        { NzConnection.NzTypeVarBinary, "NzTypeVarBinary" },
        { NzConnection.NzTypeNChar, "NzTypeNChar" },
        { NzConnection.NzTypeNVarChar, "NzTypeNVarChar" },
        { NzConnection.NzTypeJson, "NzTypeJson" },
        { NzConnection.NzTypeJsonb, "NzTypeJsonb" },
        { NzConnection.NzTypeJsonpath, "NzTypeJsonpath" }
    };
}
