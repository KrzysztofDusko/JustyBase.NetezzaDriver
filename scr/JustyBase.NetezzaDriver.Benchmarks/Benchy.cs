using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using JustyBase.NetezzaDriver;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;

namespace JustyBase.NetezzaDriver.Benchmarks;


[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net80)]
[SimpleJob(RuntimeMoniker.Net90)]
public class Benchy
{
    private const string _host = "linux.local";//nps5070
    private const string _dbName = "JUST_DATA";
    private const string _userName = "admin";
    private static readonly string _password = Environment.GetEnvironmentVariable("NZ_DEV_PASSWORD") ?? throw new InvalidOperationException("Environment variable NZ_PASSWORD is not set.");

    private const int _port = 5480;

    private DbConnection _nzNewConnection = null!;
    private DbConnection _odbcConnection = null!;

    [GlobalSetup]
    public void Setup()
    {
        _nzNewConnection = new NzConnection(_userName, _password, _host, _dbName, _port, logger: null);
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
        "SELECT 1,2,3,4,5,6,7,8,9,10 FROM JUST_DATA..FACTPRODUCTINVENTORY FI ORDER BY ROWID LIMIT 50001"
        //"SELECT 1,* FROM JUST_DATA..FACTPRODUCTINVENTORY FI ORDER BY ROWID LIMIT 10002",
        //"SELECT 2,* FROM JUST_DATA..DIMDATE DD ORDER BY ROWID LIMIT 10003",
        //"SELECT 3,* FROM JUST_DATA.._V_RELATION_COLUMN ORDER BY NAME,ATTNUM LIMIT 50004"
        )]
    public string Query { get; set; } = "";

    [Benchmark]
    public void JustyNzDriver()
    {
        ReaderValues(_nzNewConnection, Query);
    }

    [Benchmark]
    public void NzOdbc()
    {
        ReaderValues(_odbcConnection, Query);
    }

    [Benchmark]
    public void JustyNzDriverTyped()
    {
        ReadedTyped(_nzNewConnection, Query);
    }

    [Benchmark]
    public void NzOdbcTyped()
    {
        ReadedTyped(_odbcConnection, Query);
    }

    private static void ReaderValues(DbConnection dbConnection, string query)
    {
        using var cursor = dbConnection.CreateCommand();
        cursor.CommandText = query;
        using var reader = cursor.ExecuteReader();
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
                else
                {
                    var o = reader.GetValue(i);
                }

            }
        }
    }
}
