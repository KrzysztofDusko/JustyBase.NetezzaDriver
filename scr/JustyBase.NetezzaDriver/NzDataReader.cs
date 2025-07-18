using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace JustyBase.NetezzaDriver;

public sealed class NzDataReader : DbDataReader
{
    private readonly NzCommand _nzCommand;
    private readonly NzConnection _nzConnection;

    private bool _opened = true;
    private bool _needsToBeNextResultCalled = false;
    private bool _hasRows = false;
    private bool _disposed = false;

    private DataTable? _schemaTable;

    public NzDataReader(NzCommand nzCommand)
    {
        _nzCommand = nzCommand ?? throw new ArgumentNullException(nameof(nzCommand));
        _nzConnection = (NzConnection)nzCommand.Connection ?? throw new ArgumentNullException(nameof(nzCommand.Connection));

        InitializeReader();
    }

    private void InitializeReader()
    {
        while (_opened = _nzConnection.DoNextStep(_nzCommand))
        {
            if (_nzConnection.NewRowDescriptionReceived())
            {
                CheckIfRowsAreAvailable();
                break;
            }
        }
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool Read()
    {
        // Fast path for common case
        if(_disposed || !_opened || _needsToBeNextResultCalled)
        {
            return false;
        }

        while (_opened = _nzConnection.DoNextStep(_nzCommand))
        {
            if (_nzConnection.NewRowReceived())
            {
                return true;
            }
            if (_nzConnection.NewRowDescriptionReceived())
            {
                CheckIfRowsAreAvailable();
                _needsToBeNextResultCalled = true;
                return false;
            }
        }
        return false;//final stop!
    }

    public override void Close()
    {
        if (!_disposed && _opened)
        {
            base.Close();
            // Drain remaining data
            while (_opened = _nzConnection.DoNextStep(_nzCommand))
            {
                // Empty loop body - just draining
            }
        }
    }
    protected override void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            Close();
            _disposed = true;
        }
        base.Dispose(disposing);
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed()
    {
        if (_disposed) ThrowObjectDisposedException();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowObjectDisposedException() =>
        throw new ObjectDisposedException(nameof(NzDataReader));


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateOrdinal(int ordinal)
    {
        if ((uint)ordinal >= (uint)FieldCount)
        {
            ThrowOrdinalOutOfRange(ordinal);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ThrowOrdinalOutOfRange(int ordinal) =>
    throw new IndexOutOfRangeException($"Ordinal {ordinal} is out of range. Valid range is 0 to {FieldCount - 1}");

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateCanRead()
    {
        if (_disposed || !_opened) ThrowCannotRead();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowCannotRead() =>
     throw new InvalidOperationException("Reader is closed or disposed");


    public override object this[int ordinal]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            ValidateOrdinal(ordinal);
            return GetValue(ordinal);
        }
    }

    public override object this[string name]
    {
        get
        {
            ThrowIfDisposed();
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Column name cannot be null or empty", nameof(name));

            var idx = _nzCommand?.NewPreparedStatement?.Description?.FieldIndex(name);
            if (idx is int index)
            {
                return GetValue(index);
            }
            throw new IndexOutOfRangeException($"Column '{name}' not found");
        }
    }


    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowColumnNotFound(string name) =>
    throw new IndexOutOfRangeException($"Column '{name}' not found");



    public override int Depth => 0; // Netezza doesn't support nested results

    public override int FieldCount => _nzCommand.FieldCount;

    public override bool HasRows => _hasRows;

    public override bool IsClosed => !_opened || _disposed;

    public override int RecordsAffected => _nzCommand._recordsAffected;


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool GetBoolean(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _nzCommand.GetValue(ordinal).boolValue;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override byte GetByte(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _nzCommand.GetValue(ordinal).byteValue;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override char GetChar(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _nzCommand.GetValue(ordinal).charValue;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateTime GetDateTime(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _nzCommand.GetValue(ordinal).dateTimeValue;
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override decimal GetDecimal(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _nzCommand.GetValue(ordinal).decimalValue;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override double GetDouble(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _nzCommand.GetValue(ordinal).doubleValue;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override float GetFloat(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _nzCommand.GetValue(ordinal).singleValue;
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override short GetInt16(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _nzCommand.GetValue(ordinal).int16Value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int GetInt32(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _nzCommand.GetValue(ordinal).int32Value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override long GetInt64(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _nzCommand.GetValue(ordinal).int64Value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override string GetString(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _nzCommand.GetValue(ordinal).stringValue ?? string.Empty;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override object GetValue(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _nzCommand.GetValue(ordinal).GetValue();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsDBNull(int ordinal)
    {
        ValidateOrdinal(ordinal);
        ref readonly var rw = ref _nzCommand.GetValue(ordinal);
        return rw.typeCode == TypeCodeEx.DBNull || rw.typeCode == TypeCodeEx.Empty;
    }



    public override int GetValues(object[] values)
    {
        if (values == null) throw new ArgumentNullException(nameof(values));

        ValidateCanRead();

        int copyCount = Math.Min(FieldCount, values.Length);
        ref object valuesRef = ref MemoryMarshal.GetArrayDataReference(values);

        for (int i = 0; i < copyCount; i++)
        {
            Unsafe.Add(ref valuesRef, i) = GetValue(i);
        }

        return copyCount;
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        ValidateCanRead();
        ValidateOrdinal(ordinal);

        if (dataOffset < 0) throw new ArgumentOutOfRangeException(nameof(dataOffset));
        if (buffer != null && bufferOffset < 0) throw new ArgumentOutOfRangeException(nameof(bufferOffset));
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length));

        // TODO: Implement with Span<byte> for better performance
        throw new NotImplementedException("GetBytes method requires implementation");
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        ValidateCanRead();
        ValidateOrdinal(ordinal);

        if (dataOffset < 0) throw new ArgumentOutOfRangeException(nameof(dataOffset));
        if (buffer != null && bufferOffset < 0) throw new ArgumentOutOfRangeException(nameof(bufferOffset));
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length));

        // TODO: Implement with Span<char> for better performance
        throw new NotImplementedException("GetChars method requires implementation");
    }

    public override IEnumerator GetEnumerator()
    {
        // Return a simple enumerator that yields current row values
        for (int i = 0; i < FieldCount; i++)
        {
            yield return GetValue(i);
        }
    }

    public override string GetDataTypeName(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return GetFieldType(ordinal).Name;
    }



    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type GetFieldType(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _nzCommand.GetFieldType(ordinal);
    }

    public override Guid GetGuid(int ordinal)
    {
        ValidateOrdinal(ordinal);
        var value = GetValue(ordinal);
        return value is Guid guid ? guid : throw new InvalidCastException($"Cannot cast column {ordinal} to Guid");
    }


    public override string GetName(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _nzCommand.GetName(ordinal);
    }

    public override int GetOrdinal(string name)
    {
        ValidateCanRead();
        if (string.IsNullOrEmpty(name))
            throw new ArgumentException("Column name cannot be null or empty", nameof(name));

        var index = _nzCommand?.NewPreparedStatement?.Description?.FieldIndex(name);
        if (index.HasValue)
        {
            return index.Value;
        }
        throw new IndexOutOfRangeException($"Column '{name}' not found");
    }

    // Extension methods for better performance (if RowValue supports them)
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T GetFieldValue<T>(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return (T)GetValue(ordinal);
    }

    public override bool NextResult()
    {
        ThrowIfDisposed();
        _needsToBeNextResultCalled = false;
        return _opened;
    }

    public bool ShouldContinue => _opened && !_disposed;


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CheckIfRowsAreAvailable()
    {
        _nzConnection.ReadNextResponseByte();
        _hasRows = !_nzConnection.IsCommandComplete();

        if (_hasRows && _nzConnection.NewRowDescriptionStandardReceived())
        {
            _opened = _nzConnection.DoNextStep(_nzCommand);
        }
    }

    public override DataTable? GetSchemaTable()
    {
        ValidateCanRead();

        // Cache schema table for performance
        if (_schemaTable != null)
        {
            return _schemaTable;
        }

        _schemaTable = new DataTable("Schema Table");
        _schemaTable.Columns.Add("ColumnName", typeof(string));
        _schemaTable.Columns.Add("ColumnOrdinal", typeof(int));
        _schemaTable.Columns.Add("ColumnSize", typeof(Int16));
        _schemaTable.Columns.Add("NumericPrecision", typeof(int));
        _schemaTable.Columns.Add("NumericScale", typeof(int));
#pragma warning disable IL2111 // Method with parameters or return value with `DynamicallyAccessedMembersAttribute` is accessed via reflection. Trimmer can't guarantee availability of the requirements of the method.
        _schemaTable.Columns.Add("DataType", typeof(Type));
#pragma warning restore IL2111 // Method with parameters or return value with `DynamicallyAccessedMembersAttribute` is accessed via reflection. Trimmer can't guarantee availability of the requirements of the method.
        _schemaTable.Columns.Add("ProviderType", typeof(int));
        _schemaTable.Columns.Add("AllowDBNull", typeof(bool));

        _schemaTable.Columns.Add(new DataColumn("IsAutoIncrement", typeof(bool)) { DefaultValue = false });
        _schemaTable.Columns.Add(new DataColumn("IsLong", typeof(bool)) { DefaultValue = false});
        _schemaTable.Columns.Add(new DataColumn("IsReadOnly", typeof(bool)) { DefaultValue = false });


        for (int ordinal = 0; ordinal < FieldCount; ordinal++)
        {
            var currentType = GetFieldType(ordinal);
            var fieldSize = _nzConnection.CTableIFieldSizeAlternative(ordinal);
            var allowDbNull = _nzConnection.IsColumnNullable(ordinal);

            switch (currentType)
            {
                case Type t when t == typeof(decimal):
                    _schemaTable.Rows.Add([
                        _nzCommand.NewPreparedStatement!.Description![ordinal].Name
                    , ordinal + 1
                    , fieldSize
                    , _nzConnection.CTableIFieldPrecisionAlternative(ordinal)
                    , _nzConnection.CTableIFieldScaleAlternative(ordinal),
                        currentType, _nzCommand.NewPreparedStatement!.Description![ordinal].TypeOID, allowDbNull]);
                    break;
                case Type t when t == typeof(string):
                    _schemaTable.Rows.Add([_nzCommand.NewPreparedStatement!.Description![ordinal].Name
                        , ordinal + 1
                        , fieldSize
                        , -1
                        , -1
                        , currentType, _nzCommand.NewPreparedStatement!.Description![ordinal].TypeOID, allowDbNull]);
                    break;
                case Type t when t == typeof(DateTime):
                    _schemaTable.Rows.Add([_nzCommand.NewPreparedStatement!.Description![ordinal].Name
                        , ordinal + 1
                        , fieldSize
                        , -1
                        , -1
                        , currentType, _nzCommand.NewPreparedStatement!.Description![ordinal].TypeOID, allowDbNull]);
                    break;
                case Type t when t == typeof(TimeSpan):
                    _schemaTable.Rows.Add([_nzCommand.NewPreparedStatement!.Description![ordinal].Name
                        , ordinal + 1
                        , fieldSize
                        , -1
                        , -1
                        , currentType, _nzCommand.NewPreparedStatement!.Description![ordinal].TypeOID, allowDbNull]);
                    break;
                default:
                    _schemaTable.Rows.Add([_nzCommand.NewPreparedStatement!.Description![ordinal].Name
                        , ordinal + 1
                        , fieldSize
                        , -1
                        , -1
                        , currentType, _nzCommand.NewPreparedStatement!.Description![ordinal].TypeOID, allowDbNull]);
                    break;
            };
        }

        return _schemaTable;
    }
}


//CREATE TABLE TEST_NOT_NULL
//(
//ID INT NOT NULL
//)
//DISTRIBUTE ON RANDOM;

//INSERT INTO TEST_NOT_NULL
//SELECT 15