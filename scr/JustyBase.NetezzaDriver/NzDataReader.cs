using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace JustyBase.NetezzaDriver;

public sealed class NzDataReader : DbDataReader
{
    private readonly NzCommand _nzCommand;
    private readonly NzConnection _nzConnection;
    public NzDataReader(NzCommand nzCommand)
    {
        _nzCommand = nzCommand;
        _nzConnection = (NzConnection)nzCommand.Connection!;

        while (_opened = _nzConnection.DoNextStep(_nzCommand))
        {
            if (_nzConnection.NewRowDescriptionReceived())
            {
                CheckIfRowsAreAvaiable();
                break;
            }
        }
    }

    private bool _needsToBeNextResultCalled = false;
    public override bool Read()
    {
        if (IsClosed)// do not even try to read if there is no more data
        {
            return false;
        }
        if (_needsToBeNextResultCalled)
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
                CheckIfRowsAreAvaiable();
                _needsToBeNextResultCalled = true;
                return false;
            }
        }
        return false;//final stop!
    }


    public override void Close()
    {
        if (!IsClosed)
        {
            base.Close();
            while (_opened = _nzConnection.DoNextStep(_nzCommand)) ;//draing the rest of the data
        }
    }
    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => _nzCommand?.NewPreparedStatement?.Description?.FieldIndex(name) is int idx ? GetValue(idx) : throw new IndexOutOfRangeException();

    public override int Depth => throw new NotImplementedException();

    public override int FieldCount => _nzCommand.FieldCount;


    private bool _hasRows = false;
    public override bool HasRows => _hasRows;

    public override bool IsClosed => !_opened;

    public override int RecordsAffected => _nzCommand._recordsAffected;

    public override bool GetBoolean(int ordinal)
    {
        ref RowValue rw = ref _nzCommand.GetValue(ordinal);
        return rw.boolValue;
    }

    public override byte GetByte(int ordinal)
    {
        ref RowValue rw = ref _nzCommand.GetValue(ordinal);
        return rw.byteValue;
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override char GetChar(int ordinal)
    {
        ref RowValue rw = ref _nzCommand.GetValue(ordinal);
        return rw.charValue;
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override string GetDataTypeName(int ordinal)
    {
        return GetFieldType(ordinal).Name;
    }

    public override DateTime GetDateTime(int ordinal)
    {
        ref RowValue rw = ref _nzCommand.GetValue(ordinal);
        return rw.dateTimeValue;
    }

    public override decimal GetDecimal(int ordinal)
    {
        ref RowValue rw = ref _nzCommand.GetValue(ordinal);
        return rw.decimalValue;
    }

    public override double GetDouble(int ordinal)
    {
        ref RowValue rw = ref _nzCommand.GetValue(ordinal);
        return rw.doubleValue;
    }

    public override IEnumerator GetEnumerator()
    {
        throw new NotImplementedException();
    }

    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type GetFieldType(int ordinal) => _nzCommand.GetFieldType(ordinal);

    public override float GetFloat(int ordinal)
    {
        ref RowValue rw = ref _nzCommand.GetValue(ordinal);
        return rw.singleValue;
    }

    public override Guid GetGuid(int ordinal)
    {
        return (Guid) GetValue(ordinal);
    }

    public override short GetInt16(int ordinal)
    {
        //return (short) GetValue(ordinal);
        ref RowValue rw = ref _nzCommand.GetValue(ordinal);
        return rw.int16Value;
    }

    public override int GetInt32(int ordinal)
    {
        //return (int)GetValue(ordinal);
        ref RowValue rw = ref _nzCommand.GetValue(ordinal);
        return rw.int32Value;
    }

    public override long GetInt64(int ordinal)
    {
        //return (long)GetValue(ordinal);
        ref RowValue rw = ref _nzCommand.GetValue(ordinal);
        return rw.int64Value;
    }

    public override string GetName(int ordinal) => _nzCommand.GetName(ordinal);


    public override int GetOrdinal(string name)
    {
        throw new NotImplementedException();
    }
    //TODO emty string.int etc test.
    public override string GetString(int ordinal)
    {
        //return (string) GetValue(ordinal);
        ref RowValue rw = ref _nzCommand.GetValue(ordinal);
        return rw.stringValue;
    }

    public override object GetValue(int ordinal)
    {
        ref RowValue rw = ref _nzCommand.GetValue(ordinal);
        return rw.GetValue();
    }

    public override int GetValues(object[] values)
    {
        int i;
        for (i = 0; i < FieldCount && i < values.Length; i++)
        {
            values[i] = GetValue(i);
        }
        return i;
    }

    public override bool IsDBNull(int ordinal)
    {
        ref RowValue rw = ref _nzCommand.GetValue(ordinal);
        return rw.typeCode == TypeCodeEx.DBNull || rw.typeCode == TypeCodeEx.Empty;
    }

    public override bool NextResult()
    {
        _needsToBeNextResultCalled = false;
        return _opened;
    }

    private bool _opened = true;
    public bool ShouldContinue => _opened;

    /// <summary>
    /// Check if there are more rows to read
    /// </summary>
    private void CheckIfRowsAreAvaiable()
    {
        _nzConnection.ReadNextResponseByte();
        if (_nzConnection.IsCommandComplete())
        {
            _hasRows = false;
        }
        else
        {
            _hasRows = true;
        }
        if (HasRows && _nzConnection.NewRowDescriptionStandardReceived())
        {
            _opened = _nzConnection.DoNextStep(_nzCommand);
        }
    }

    //todo: implement more columns
    //todo: unit test

    public override DataTable? GetSchemaTable()
    {
        DataTable dt = new DataTable("Schema Table");
        dt.Columns.Add("ColumnName", typeof(string));
        dt.Columns.Add("ColumnOrdinal", typeof(int));
        dt.Columns.Add("ColumnSize", typeof(Int16));
        dt.Columns.Add("NumericPrecision", typeof(int));
        dt.Columns.Add("NumericScale", typeof(int));
#pragma warning disable IL2111 // Method with parameters or return value with `DynamicallyAccessedMembersAttribute` is accessed via reflection. Trimmer can't guarantee availability of the requirements of the method.
        dt.Columns.Add("DataType", typeof(Type));
#pragma warning restore IL2111 // Method with parameters or return value with `DynamicallyAccessedMembersAttribute` is accessed via reflection. Trimmer can't guarantee availability of the requirements of the method.
        dt.Columns.Add("ProviderType", typeof(int));
        dt.Columns.Add("AllowDBNull", typeof(bool));

        dt.Columns.Add(new DataColumn("IsAutoIncrement", typeof(bool)) { DefaultValue = false });
        dt.Columns.Add(new DataColumn("IsLong", typeof(bool)) { DefaultValue = false});
        dt.Columns.Add(new DataColumn("IsReadOnly", typeof(bool)) { DefaultValue = false });


        for (int ordinal = 0; ordinal < FieldCount; ordinal++)
        {
            var currentType = GetFieldType(ordinal);
            var fieldSize = _nzConnection.CTableIFieldSizeAlternative(ordinal);
            var allowDbNull = _nzConnection.IsColumnNullable(ordinal);

            switch (currentType)
            {
                case Type t when t == typeof(decimal):
                    dt.Rows.Add([
                        _nzCommand.NewPreparedStatement!.Description![ordinal].Name
                    , ordinal + 1
                    , fieldSize
                    , _nzConnection.CTableIFieldPrecisionAlternative(ordinal)
                    , _nzConnection.CTableIFieldScaleAlternative(ordinal),
                        currentType, _nzCommand.NewPreparedStatement!.Description![ordinal].TypeOID, allowDbNull]);
                    break;
                case Type t when t == typeof(string):
                    dt.Rows.Add([_nzCommand.NewPreparedStatement!.Description![ordinal].Name
                        , ordinal + 1
                        , fieldSize
                        , -1
                        , -1
                        , currentType, _nzCommand.NewPreparedStatement!.Description![ordinal].TypeOID, allowDbNull]);
                    break;
                case Type t when t == typeof(DateTime):
                    dt.Rows.Add([_nzCommand.NewPreparedStatement!.Description![ordinal].Name
                        , ordinal + 1
                        , fieldSize
                        , -1
                        , -1
                        , currentType, _nzCommand.NewPreparedStatement!.Description![ordinal].TypeOID, allowDbNull]);
                    break;
                case Type t when t == typeof(TimeSpan):
                    dt.Rows.Add([_nzCommand.NewPreparedStatement!.Description![ordinal].Name
                        , ordinal + 1
                        , fieldSize
                        , -1
                        , -1
                        , currentType, _nzCommand.NewPreparedStatement!.Description![ordinal].TypeOID, allowDbNull]);
                    break;
                default:
                    dt.Rows.Add([_nzCommand.NewPreparedStatement!.Description![ordinal].Name
                        , ordinal + 1
                        , fieldSize
                        , -1
                        , -1
                        , currentType, _nzCommand.NewPreparedStatement!.Description![ordinal].TypeOID, allowDbNull]);
                    break;
            };
        }

        return dt;
    }
}


//CREATE TABLE TEST_NOT_NULL
//(
//ID INT NOT NULL
//)
//DISTRIBUTE ON RANDOM;

//INSERT INTO TEST_NOT_NULL
//SELECT 15