using BenchmarkDotNet.Attributes;
using System.Data.Common;
using System.Data.Odbc;

namespace JustyBase.NetezzaDriver.Benchmarks;

[MemoryDiagnoser]
public class ExternalBench
{
    private const string _host = "linux.local";
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


    [Benchmark]
    public void ExternalUnloadAndLoadNz()
    {
        TestOneTableNetezza(_nzNewConnection, "DIMCUSTOMER");
    }
    private static void TestOneTableNetezza(DbConnection connection, string tablename)
    {
        using NzCommand cursor = (NzCommand)connection.CreateCommand();
        var externalPath = $"E:\\{tablename}.dat";
        var tablenameOrg = $"JUST_DATA..{tablename}";
        var tablenameNew = $"{tablenameOrg}_FROM_EXTERNAL";

        cursor.CommandText = $"DROP TABLE {tablenameNew} IF EXISTS";
        cursor.ExecuteNonQuery();
        cursor.CommandText = "DROP TABLE ET_TEMP IF EXISTS";
        cursor.ExecuteNonQuery();
        cursor.CommandText = $"create external table ET_TEMP '{externalPath}' using ( remotesource 'python' delimiter '|') as select * from {tablenameOrg}";
        cursor.ExecuteNonQuery();
        cursor.CommandText = $"DROP TABLE {tablenameNew} IF EXISTS";
        cursor.ExecuteNonQuery();
        cursor.CommandText = $"CREATE TABLE {tablenameNew} AS (SELECT * FROM {tablenameOrg} WHERE 1=2)";
        cursor.ExecuteNonQuery();
        cursor.CommandText = $"INSERT INTO {tablenameNew}  SELECT * FROM EXTERNAL '{externalPath}' " +
            @"using ( remotesource 'python' delimiter '|' socketbufsize 8388608 ctrlchars 'yes'  encoding 'internal' timeroundnanos 'yes' crinstring 'off' logdir E:\logs\)";
        cursor.ExecuteNonQuery();
    }


    [Benchmark]
    public void ExternalUnloadAndLoadOdbc()
    {
        TestOneTableOdbc(_odbcConnection, "DIMCUSTOMER");
    }
    private static void TestOneTableOdbc(DbConnection connection, string tablename)
    {
        using var cursor = connection.CreateCommand();
        var externalPath = $"E:\\{tablename}.dat";
        var tablenameOrg = $"JUST_DATA..{tablename}";
        var tablenameNew = $"{tablenameOrg}_FROM_EXTERNAL";

        cursor.CommandText = $"DROP TABLE {tablenameNew} IF EXISTS";
        cursor.ExecuteNonQuery();
        cursor.CommandText = "DROP TABLE ET_TEMP IF EXISTS";
        cursor.ExecuteNonQuery();
        cursor.CommandText = $"create external table ET_TEMP '{externalPath}' using ( remotesource 'odbc' delimiter '|') as select * from {tablenameOrg}";
        cursor.ExecuteNonQuery();
        cursor.CommandText = $"DROP TABLE {tablenameNew} IF EXISTS";
        cursor.ExecuteNonQuery();
        cursor.CommandText = $"CREATE TABLE {tablenameNew} AS (SELECT * FROM {tablenameOrg} WHERE 1=2)";
        cursor.ExecuteNonQuery();
        cursor.CommandText = $"INSERT INTO {tablenameNew}  SELECT * FROM EXTERNAL '{externalPath}' " +
            @"using ( remotesource 'odbc' delimiter '|' socketbufsize 8388608 ctrlchars 'yes'  encoding 'internal' timeroundnanos 'yes' crinstring 'off' logdir E:\logs\)";
        cursor.ExecuteNonQuery();
    }
}
