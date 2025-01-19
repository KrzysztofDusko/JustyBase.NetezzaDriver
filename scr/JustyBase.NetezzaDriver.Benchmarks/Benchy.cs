using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using System.Data.Common;
using System.Data.Odbc;

namespace JustyBase.NetezzaDriver.Benchmarks;


[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net90)]
public class Benchy
{
    private const string _host = "linux.local";//nps5070
    private const string _dbName = "JUST_DATA";
    private const string _userName = "admin";
    private static readonly string _password = Environment.GetEnvironmentVariable("NZ_DEV_PASSWORD") ?? throw new InvalidOperationException("Environment variable NZ_PASSWORD is not set.");

    private const int _port = 5480;

    private NzConnection _nzNewConnection = null!;
    private DbConnection _odbcConnection = null!;

    [GlobalSetup]
    public void Setup()
    {
        _nzNewConnection = new NzConnection(_userName, _password, _host, _dbName, _port);
        _nzNewConnection.Open();

        _odbcConnection = new OdbcConnection($"Driver={{NetezzaSQL}};servername={_host};port={_port};database={_dbName};username={_userName};password={_password}");
        _odbcConnection.Open();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _nzNewConnection.Dispose();
        _odbcConnection.Dispose();
    }
    [Params(
        "/*1*/SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY FI ORDER BY ROWID LIMIT 50000",
        "/*2*/SELECT * FROM JUST_DATA..DIMDATE DD ORDER BY ROWID LIMIT 10000"
        //"/*3*/SELECT 1,2,3,'abc'::char(10),'ąężźń'::nchar(12),'aaaa' || ((RANDOM()*100)::INT),'xaaa' || ((RANDOM()*100)::INT),'yaaa' || ((RANDOM()*100)::INT),'zaaa' || ((RANDOM()*100)::INT) FROM JUST_DATA..FACTPRODUCTINVENTORY DD ORDER BY ROWID LIMIT 50000"
        //"/*4*/SELECT 1,2,3,'a' || ((RANDOM()*100)::INT),'b' || ((RANDOM()*100)::INT),'c' || ((RANDOM()*100)::INT) FROM JUST_DATA.._V_RELATION_COLUMN ORDER BY NAME,ATTNUM LIMIT 10000"
        //"/*5*/SELECT '12:00:00'::TIMETZ,'14:13:12.4321+11:15'::TIMETZ FROM JUST_DATA..DIMCURRENCY ORDER BY ROWID",
        //"/*6*/SELECT '12:00:00'::TIME, '12:00:00-12'::TIMETZ, '12:00:00+12'::TIMETZ,'14:13:12.4321+11:15'::TIMETZ FROM JUST_DATA..DIMACCOUNT LIMIT 1",
        //"/*7*/SELECT RANDOM() FROM JUST_DATA..FACTPRODUCTINVENTORY ORDER BY ROWID LIMIT 10000"
        //"/*8*/SELECT * FROM JUST_DATA..DIMCURRENCY ORDER BY ROWID",
        )]
    public string Query { get; set; } = "";

    [Benchmark]
    public void JustyNzDriver()
    {
        ReaderValues(_nzNewConnection, Query);
    }
    [Benchmark]
    public void JustyNzDriverTyped()
    {
        ReadedTyped(_nzNewConnection, Query);
    }
    [Benchmark]
    public void NzOdbc()
    {
        ReaderValues(_odbcConnection, Query);
    }

    //[Benchmark] same as NzOdbc
    public void NzOdbcTyped()
    {
        ReadedTyped(_odbcConnection, Query);
    }

    private static void ReaderValues(DbConnection dbConnection, string query)
    {
        using var nzCommand = dbConnection.CreateCommand();
        nzCommand.CommandText = query;
        using var reader = nzCommand.ExecuteReader();
        while (reader.Read())
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var o = reader.GetValue(i);
            }
        }
    }

    private static void ReadedTyped(DbConnection dbConnection, string query)
    {
        using var cmd = dbConnection.CreateCommand();
        cmd.CommandText = query;
        using var reader = cmd.ExecuteReader();

        while (reader.Read())
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (reader.IsDBNull(i))
                {
                    continue;
                }

                if (reader.GetFieldType(i) == typeof(int))
                {
                    var o = reader.GetInt32(i);
                }
                else if (reader.GetFieldType(i) == typeof(long))
                {
                    var o = reader.GetInt64(i);
                }
                else if (reader.GetFieldType(i) == typeof(DateTime))
                {
                    var o = reader.GetDateTime(i);
                }
                else if (reader.GetFieldType(i) == typeof(decimal))
                {
                    var o = reader.GetDecimal(i);
                }
                else if (reader.GetFieldType(i) == typeof(float))
                {
                    var o = reader.GetFloat(i);
                }
                else if (reader.GetFieldType(i) == typeof(double))
                {
                    var o = reader.GetDouble(i);
                }
                else if (reader.GetFieldType(i) == typeof(string))
                {
                    var o = reader.GetString(i);
                }
                else if (reader.GetFieldType(i) == typeof(bool))
                {
                    var o = reader.GetBoolean(i);
                }
                else if (reader.GetFieldType(i) == typeof(byte))
                {
                    var o = reader.GetByte(i);
                }
                else if (reader.GetFieldType(i) == typeof(char))
                {
                    var o = reader.GetChar(i);
                }
                else if (reader.GetFieldType(i) == typeof(Guid))
                {
                    var o = reader.GetGuid(i);
                }
                else if (reader.GetFieldType(i) == typeof(short))
                {
                    var o = reader.GetInt16(i);
                }
                else
                {
                    var o = reader.GetValue(i);
                }
            }
        }
    }
}


