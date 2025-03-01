using JustyBase.NetezzaDriver.Logging;
using System.Security.Authentication;
using Xunit.Abstractions;

namespace JustyBase.NetezzaDriver.Tests;

public class SslTest
{
    private static readonly string _password = Environment.GetEnvironmentVariable("NZ_DEV_PASSWORD") ?? throw new InvalidOperationException("Environment variable NZ_PASSWORD is not set.");

    private readonly ITestOutputHelper _output;
    public SslTest(ITestOutputHelper output)
    {
        _output = output;
    }
    [Fact]
    public void BasicTests()
    {
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA",
            securityLevel: 3, sslCerFilePath: @"D:\server-cert.pem");
        //this cert file is invalid
        Assert.Throws<AuthenticationException>(() =>
        {
            connection.Open();
            using var command = connection.CreateCommand();
        });
    }

    [Fact]
    public void BasicTests2()
    {
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA",
            securityLevel: 3, sslCerFilePath: @"D:\server-cert.pem", logger: new SimpleNzLogger());
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 15 FROM JUST_DATA..DIMDATE";
        using var rdr = command.ExecuteReader();
        rdr.Read();
        var res = rdr.GetValue(0);
        Assert.Equal(15, (int)res);
    }
}
