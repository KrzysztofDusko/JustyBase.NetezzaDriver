using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using JustyBase.NetezzaDriver;
using System.Data;
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

    private DbConnection _nzNewConnection = null!;
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
        "SELECT 1,2,3,4,5,6,7,8,9,10,* FROM JUST_DATA..FACTPRODUCTINVENTORY FI ORDER BY ROWID LIMIT 10001",
        //"SELECT 1,* FROM JUST_DATA..FACTPRODUCTINVENTORY FI ORDER BY ROWID LIMIT 10002",
        //"SELECT 2,* FROM JUST_DATA..DIMDATE DD ORDER BY ROWID LIMIT 10003",
        //"SELECT 8,'aaaa' FROM JUST_DATA..FACTPRODUCTINVENTORY DD ORDER BY ROWID LIMIT 10003",
        "SELECT * FROM JUST_DATA.._V_RELATION_COLUMN ORDER BY NAME,ATTNUM LIMIT 10004"
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

//| Method             | Query                | Mean     | Error   | StdDev  | Gen0      | Allocated  |
//|------------------- |--------------------- |---------:|--------:|--------:|----------:|-----------:|
//| JustyNzDriver      | SELEC(...)10004 [76] | 171.1 ms | 2.65 ms | 2.48 ms |  666.6667 | 7841.31 KB |
//| NzOdbc             | SELEC(...)10004 [76] | 160.5 ms | 3.18 ms | 4.13 ms | 1000.0000 | 8233.31 KB |
//| JustyNzDriverTyped | SELEC(...)10004 [76] | 160.5 ms | 3.18 ms | 5.23 ms |  500.0000 | 4793.16 KB |
//| NzOdbcTyped        | SELEC(...)10004 [76] | 155.4 ms | 1.43 ms | 1.27 ms | 1000.0000 | 8233.31 KB |

//| JustyNzDriver      | SELEC(...)10001 [96] | 125.1 ms | 2.42 ms | 1.89 ms |  500.0000 | 4148.57 KB |
//| NzOdbc             | SELEC(...)10001 [96] | 141.4 ms | 2.77 ms | 3.98 ms |  500.0000 | 4928.16 KB |
//| JustyNzDriverTyped | SELEC(...)10001 [96] | 130.8 ms | 2.61 ms | 7.37 ms |         - |  788.86 KB |
//| NzOdbcTyped        | SELEC(...)10001 [96] | 145.0 ms | 2.85 ms | 4.84 ms |  500.0000 | 4928.16 KB |

//TODO string pooling
