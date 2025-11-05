using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using Xunit.Abstractions;

namespace JustyBase.NetezzaDriver.Tests;


public class CommandAbortTest
{
    private static readonly string _password = Environment.GetEnvironmentVariable("NZ_DEV_PASSWORD") ?? throw new InvalidOperationException("Environment variable NZ_PASSWORD is not set.");

    private readonly ITestOutputHelper _output;
    public CommandAbortTest(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task AbortTest1()
    {
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA");
        await AbortTestHelper(connection);
    }

    [Fact]
    public async Task AbortTestWithSSL()
    {
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA", securityLevel: SecurityLevelCode.OnlySecuredSession, sslCerFilePath: @"D:\DEV\Others\keys\server-cert.pem", loggerFactory: new NullLoggerFactory());
        await AbortTestHelper(connection);
    }

    private async Task AbortTestHelper(NzConnection connection)
    {
        connection.Open();
        connection.CommandTimeout = TimeSpan.FromSeconds(120);

        using var command = connection.CreateCommand();
        command.CommandText = "show connection";
        command.ExecuteNonQuery();
        using var rdr1 = command.ExecuteReader();
        while (rdr1.Read())
        {
            for (int i = 0; i < rdr1.FieldCount; i++)
            {
                Debug.WriteLine(rdr1.GetValue(i)?.ToString() ?? "" + "|");
            }
            Debug.WriteLine("");
        }

        command.CommandText = "create temp table abc as (select now() as col1, random() as col2)";
        command.ExecuteNonQuery();



        Stopwatch stopwatch = Stopwatch.StartNew();
        for (int i = 0; i < 3; i++)
        {
            var longRunningQuery = Task.Run(async () =>
            {
                try
                {
                    stopwatch.Restart();
                    _output.WriteLine($"STARTED PID = {connection.Pid}");
                    command.CommandText = _heavySql;
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Assert.True(stopwatch.Elapsed > TimeSpan.FromSeconds(5.5));
                    Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(9.5));
                    _output.WriteLine(ex.Message);
                    _output.WriteLine("AFTER : " + stopwatch.Elapsed.ToString());
                    await Task.Delay(500);
                    command.CommandText = $"SELECT count(1) FROM _V_SESSION where STATUS = 'active' and PID = {connection.Pid} AND COMMAND LIKE '{_heavySql[0..10]}%'";
                    using var rdr = command.ExecuteReader();
                    rdr.Read();
                    Assert.Equal(0, rdr.GetInt64(0));
                    //Console.WriteLine("CHECK QUERIES ON DATABASE SIDE TO COFIRM !! (3 s)");
                    //await Task.Delay(3_000);
                    //Console.WriteLine();
                    //Console.WriteLine();
                    await Task.Delay(50);
                }
            });
            await Task.Delay(6000);
            connection.CancelQuery();
            await longRunningQuery;
        }

        _output.WriteLine($"END {stopwatch.Elapsed}");
        //await Task.Delay(200);
        command.CommandText = "SELECT * FROM ABC";
        using var rdr = command.ExecuteReader();
        rdr.Read();//do not throws.
    }


    private const string _heavySql =
    """
        SELECT     
        F1.PRODUCTKEY    
        , COUNT(DISTINCT (F1.PRODUCTKEY / F2.PRODUCTKEY))    
        FROM     
        ( SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY LIMIT 10000) F1,    
        ( SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY LIMIT 10000) F2    
        GROUP BY 1    
        LIMIT 500    
    """;
}
