using System.Collections;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace JustyBase.NetezzaDriver;

public sealed class NzDataReader : DbDataReader
{
    private readonly NzCommand _cursor;
    private readonly NzConnection _nzConnection;
    public NzDataReader(NzCommand cursor)
    {
        _cursor = cursor;
        _nzConnection = (NzConnection)cursor.Connection!;

        while (_opened = _nzConnection.DoNextStep(_cursor))
        {
            if (_nzConnection.NewRowDescriptionReceived())
            {
                break;
            }
        }
    }

    public override void Close()
    {
        if (!IsClosed)
        {
            base.Close();
            while (_opened = _nzConnection.DoNextStep(_cursor)) ;//draing the rest of the data
        }
    }
    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => throw new NotImplementedException();

    public override int Depth => throw new NotImplementedException();

    public override int FieldCount => _cursor.FieldCount;

    public override bool HasRows => throw new NotImplementedException();

    public override bool IsClosed => !_opened;

    public override int RecordsAffected => _cursor._recordsAffected;

    public override bool GetBoolean(int ordinal)
    {
        ref RowValue rw = ref _cursor.GetValue(ordinal);
        return rw.boolValue;
    }

    public override byte GetByte(int ordinal)
    {
        ref RowValue rw = ref _cursor.GetValue(ordinal);
        return rw.byteValue;
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override char GetChar(int ordinal)
    {
        ref RowValue rw = ref _cursor.GetValue(ordinal);
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
        ref RowValue rw = ref _cursor.GetValue(ordinal);
        return rw.dateTimeValue;
    }

    public override decimal GetDecimal(int ordinal)
    {
        ref RowValue rw = ref _cursor.GetValue(ordinal);
        return rw.decimalValue;
    }

    public override double GetDouble(int ordinal)
    {
        ref RowValue rw = ref _cursor.GetValue(ordinal);
        return rw.doubleValue;
    }

    public override IEnumerator GetEnumerator()
    {
        throw new NotImplementedException();
    }

    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type GetFieldType(int ordinal) => _cursor.GetFieldType(ordinal);

    public override float GetFloat(int ordinal)
    {
        ref RowValue rw = ref _cursor.GetValue(ordinal);
        return rw.singleValue;
    }

    public override Guid GetGuid(int ordinal)
    {
        return (Guid) GetValue(ordinal);
    }

    public override short GetInt16(int ordinal)
    {
        //return (short) GetValue(ordinal);
        ref RowValue rw = ref _cursor.GetValue(ordinal);
        return rw.int16Value;
    }

    public override int GetInt32(int ordinal)
    {
        //return (int)GetValue(ordinal);
        ref RowValue rw = ref _cursor.GetValue(ordinal);
        return rw.int32Value;
    }

    public override long GetInt64(int ordinal)
    {
        //return (long)GetValue(ordinal);
        ref RowValue rw = ref _cursor.GetValue(ordinal);
        return rw.int64Value;
    }

    public override string GetName(int ordinal) => _cursor.GetName(ordinal);


    public override int GetOrdinal(string name)
    {
        throw new NotImplementedException();
    }
    //TODO emty string.int etc test.
    public override string GetString(int ordinal)
    {
        //return (string) GetValue(ordinal);
        ref RowValue rw = ref _cursor.GetValue(ordinal);
        return rw.stringValue;
    }

    public override object GetValue(int ordinal)
    {
        ref RowValue rw = ref _cursor.GetValue(ordinal);
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
        ref RowValue rw = ref _cursor.GetValue(ordinal);
        return rw.typeCode == TypeCodeEx.DBNull || rw.typeCode == TypeCodeEx.Empty;
    }

    public override bool NextResult()
    {
        return _opened;
    }

    private bool _opened = true;
    public bool ShouldContinue => _opened;
    public override bool Read()
    {
        if (IsClosed)// do not even try to read if there is no more data
        {
            return false;
        }

        while (_opened = _nzConnection.DoNextStep(_cursor))
        {
            if (_nzConnection.NewRowReceived())
            {
                return true;
            }
            if (_nzConnection.NewRowDescriptionReceived())
            {
                return false;
            }
        }

        return false;//final stop!
    }
}
