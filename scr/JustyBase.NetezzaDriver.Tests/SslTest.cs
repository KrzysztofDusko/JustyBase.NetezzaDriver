using Microsoft.Extensions.Logging.Abstractions;
using System.Security.Authentication;
using Xunit.Abstractions;

namespace JustyBase.NetezzaDriver.Tests;

[Collection("Sequential")]
public class SslTest
{
    private readonly ITestOutputHelper _output;
    public SslTest(ITestOutputHelper output)
    {
        _output = output;
    }
    [Fact]
    public void BasicTests()
    {
        using NzConnection connection = new NzConnection(Config.UserName, Config.Password, Config.Host, Config.DbName,
            securityLevel: SecurityLevelCode.OnlySecuredSession, sslCerFilePath: @"D:\DEV\Others\keys\server-cert.pem");
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
        using NzConnection connection = new NzConnection(Config.UserName, Config.Password, Config.Host, Config.DbName,
            securityLevel: SecurityLevelCode.OnlySecuredSession, sslCerFilePath: @"D:\DEV\Others\keys\server-cert.pem", loggerFactory: new NullLoggerFactory());
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 15 FROM JUST_DATA..DIMDATE";
        using var rdr = command.ExecuteReader();
        rdr.Read();
        var res = rdr.GetValue(0);
        Assert.Equal(15, (int)res);
    }
}


// https://www.ibm.com/docs/en/netezza?topic=npssac-netezza-performance-server-client-encryption-security-1
// https://www.ibm.com/docs/en/netezza?topic=cnpshac-show-connection-records-1
//sudo nano /nz/simdata/postgresql.conf
//./simdata/postgresql.conf
//server_cert_file='/nz/simdata/security/server-cert.pem'
//server_key_file='/nz/simdata/security/server-key.pem'