﻿namespace JustyBase.NetezzaDriver.Tests;

public class NoticeTests
{
    private static readonly string _password = Environment.GetEnvironmentVariable("NZ_DEV_PASSWORD") ?? throw new InvalidOperationException("Environment variable NZ_PASSWORD is not set.");

    public NoticeTests()
    {
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA", 5480);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "CREATE OR REPLACE PROCEDURE JUST_DATA.ADMIN.CUSTOMER_DOTNET() RETURNS INTEGER EXECUTE AS OWNER LANGUAGE NZPLSQL AS BEGIN_PROC BEGIN RAISE NOTICE 'The customer name is alpha'; RAISE NOTICE 'The customer location is beta'; END; END_PROC;";
        command.ExecuteNonQuery();
    }

    [Fact]
    public void BasicNoticeTests()
    {
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA", 5480);
        connection.Open();
        using var command = connection.CreateCommand();
        List<string> notices = new List<string>();
        connection.NoticeReceived += (o,e) =>
        {
            notices.Add(e.Message);
        };
        command.CommandText = "CALL CUSTOMER_DOTNET();";
        command.ExecuteNonQuery();
        var expected = new List<string>() { "The customer name is alpha\n", "The customer location is beta\n" };
        Assert.Equal(expected, notices);
    }
}
