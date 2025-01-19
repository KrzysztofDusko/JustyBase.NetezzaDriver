using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace JustyBase.NetezzaDriver.Utility;
//https://github.com/danbarua/Npgsql/blob/master/Npgsql/PGUtil.cs


/// <summary>
/// Provides utility methods for reading and writing from and to streams.
/// </summary>
internal static class PGUtil
{
    /// <summary>
    /// Reads a 32-bit integer from the specified stream.
    /// </summary>
    /// <param name="stream">The stream to read from.</param>
    /// <returns>The 32-bit integer read from the stream.</returns>
    internal static int ReadInt32(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[4];
        stream.ReadExactly(buffer);
        var result = BitConverter.ToInt32(buffer);
        return BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(result) : result;
    }

    /// <summary>
    /// Reads a 16-bit integer from the specified stream.
    /// </summary>
    /// <param name="stream">The stream to read from.</param>
    /// <returns>The 16-bit integer read from the stream.</returns>
    internal static short ReadInt16(Stream stream)
    {
        Span<byte> buffer = stackalloc byte[2];
        stream.ReadExactly(buffer);
        var result = BitConverter.ToInt16(buffer);
        return BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(result) : result;
    }

    /// <summary>
    /// Writes a 16-bit integer to the specified stream.
    /// </summary>
    /// <param name="stream">The stream to write to.</param>
    /// <param name="value">The 16-bit integer to write.</param>
    internal static void WriteInt16(Stream stream, short value)
    {
        value = BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(value) : value;
        Span<byte> bytes = stackalloc byte[sizeof(short)];
        Unsafe.As<byte, short>(ref bytes[0]) = value;
        stream.Write(bytes);
    }

    /// <summary>
    /// Writes a 32-bit integer to the specified stream.
    /// </summary>
    /// <param name="stream">The stream to write to.</param>
    /// <param name="value">The 32-bit integer to write.</param>
    internal static void WriteInt32(Stream stream, int value)
    {
        value = BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(value) : value;
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        Unsafe.As<byte, int>(ref bytes[0]) = value;
        stream.Write(bytes);
    }

    /// <summary>
    /// Skips a specified number of bytes in the stream.
    /// </summary>
    /// <param name="stream">The stream to skip bytes in.</param>
    /// <param name="num">The number of bytes to skip. Default is 4.</param>
    internal static void Skip4Bytes(Stream stream, int num = 4)
    {
        Span<byte> buffer = stackalloc byte[num];
        //PGUtil.CheckedStreamRead(stream, buffer, 0, 4);
        stream.ReadExactly(buffer);
    }
}
