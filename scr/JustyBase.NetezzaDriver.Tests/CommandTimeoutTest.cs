using System.Diagnostics;
using Xunit.Abstractions;

namespace JustyBase.NetezzaDriver.Tests;

public class CommandTimeoutTest
{
    private static readonly string _password = Environment.GetEnvironmentVariable("NZ_DEV_PASSWORD") ?? throw new InvalidOperationException("Environment variable NZ_PASSWORD is not set.");

    private readonly ITestOutputHelper _output;
    public CommandTimeoutTest(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task CommandTimeoutTest1()
    {
        Console.WriteLine("### CommandTimeoutManualTest ###");
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA");
        connection.Open();
        connection.CommandTimeout = TimeSpan.FromSeconds(4);

        using var cursor = connection.CreateCommand();
        cursor.CommandText = "create temp table abc as (select now() as jeden, random() as dwa)";
        cursor.ExecuteNonQuery();

        for (int i = 0; i < 5; i++)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            try
            {
                stopwatch.Restart();
                _output.WriteLine($"STARTED PID = {connection.Pid}");
                cursor.CommandText = _heavySql;
                cursor.ExecuteNonQuery();
            }
            catch (Exception ex1)
            {
                _output.WriteLine(ex1.Message);
                _output.WriteLine("AFTER : " + stopwatch.Elapsed.ToString());
                //aborted after about connection.ConnectionTimeout seconds
                Assert.True(stopwatch.Elapsed > TimeSpan.FromSeconds(3.5));
                Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(8.5));

                cursor.CommandText = $"SELECT count(1) FROM _V_SESSION where STATUS = 'active' and PID = {connection.Pid} and command LIKE 'SELECT F1%'";
                using var rdr2 = cursor.ExecuteReader();
                rdr2.Read();
                Assert.Equal(0, rdr2.GetInt64(0));
       
                //_output.WriteLine("CHECK QUERIES ON DATABASE SIDE TO COFIRM !! (3 s)");
                //await Task.Delay(3_000);
                //_output.WriteLine();
                //_output.WriteLine();
                await Task.Delay(50);
            }
        }

        //await Task.Delay(200);
        cursor.CommandText = "SELECT * FROM ABC";
        using var rdr = cursor.ExecuteReader();
        rdr.Read();//do not throws.

    }
    //must run at least 4 seconds
    private string _heavySql =
    """
        SELECT     
        F1.PRODUCTKEY    
        , COUNT(DISTINCT (F1.PRODUCTKEY / F2.PRODUCTKEY))    
        FROM     
        ( SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY LIMIT 25000) F1,    
        ( SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY LIMIT 25000) F2    
        GROUP BY 1    
        LIMIT 500    
        """;


}