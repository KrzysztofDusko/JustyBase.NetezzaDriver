﻿using BenchmarkDotNet.Attributes;
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

    private NzConnection _nzNewConnection = null!;//default = python
    private NzConnection _nzNewConnectionDotnet = null!;
    private NzConnection _nzNewConnectionOdbc= null!;
    private DbConnection _odbcConnection = null!;

    [GlobalSetup]
    public void Setup()
    {
        _nzNewConnection = new NzConnection(_userName, _password, _host, _dbName, _port);
        _nzNewConnection.Open();

        _nzNewConnectionDotnet = new NzConnection(_userName, _password, _host, _dbName, _port);
        _nzNewConnectionDotnet.Open(ClientTypeId.SqlDotnet);

        _nzNewConnectionOdbc = new NzConnection(_userName, _password, _host, _dbName, _port);
        _nzNewConnectionOdbc.Open(ClientTypeId.SqlOdbc);

        _odbcConnection = new OdbcConnection($"Driver={{NetezzaSQL}};servername={_host};port={_port};database={_dbName};username={_userName};password={_password}");
        _odbcConnection.Open();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _nzNewConnection.Dispose();
        _nzNewConnectionDotnet.Dispose();
        _nzNewConnectionOdbc.Dispose();
        _odbcConnection.Dispose();
    }


    [Benchmark]
    public void ExternalUnloadAndLoadNz()
    {
        TestOneExternalTable(_nzNewConnection, "DIMCUSTOMER","python");
    }
    //[Benchmark]
    public void ExternalUnloadAndLoadNzDotnet()
    {
        TestOneExternalTable(_nzNewConnectionDotnet, "DIMCUSTOMER", "dotnet");
    }
    //[Benchmark]
    public void ExternalUnloadAndLoadNzOdbc()
    {
        TestOneExternalTable(_nzNewConnectionOdbc, "DIMCUSTOMER", "odbc");
    }

    [Benchmark]
    public void ExternalUnloadAndLoadOriginalOdbc()
    {
        TestOneExternalTable(_odbcConnection, "DIMCUSTOMER","odbc");
    }
    private static void TestOneExternalTable(DbConnection connection, string tablename, string driverName = "odbc")
    {
        using var command = connection.CreateCommand();
        var externalPath = $"E:\\{tablename}.dat";
        var tablenameOrg = $"JUST_DATA..{tablename}";
        var tablenameNew = $"{tablenameOrg}_FROM_EXTERNAL";

        command.CommandText = $"DROP TABLE {tablenameNew} IF EXISTS";
        command.ExecuteNonQuery();
        command.CommandText = "DROP TABLE ET_TEMP IF EXISTS";
        command.ExecuteNonQuery();
        command.CommandText = $"create external table ET_TEMP '{externalPath}' using ( remotesource '{driverName}' delimiter '|') as select * from {tablenameOrg}";
        command.ExecuteNonQuery();
        command.CommandText = $"DROP TABLE {tablenameNew} IF EXISTS";
        command.ExecuteNonQuery();
        command.CommandText = $"CREATE TABLE {tablenameNew} AS (SELECT * FROM {tablenameOrg} WHERE 1=2)";
        command.ExecuteNonQuery();
        command.CommandText = $"INSERT INTO {tablenameNew}  SELECT * FROM EXTERNAL '{externalPath}' " +
            @$"using ( remotesource '{driverName}' delimiter '|' socketbufsize 8388608 ctrlchars 'yes'  encoding 'internal' timeroundnanos 'yes' crinstring 'off' logdir E:\logs\)";
        command.ExecuteNonQuery();
    }
}
