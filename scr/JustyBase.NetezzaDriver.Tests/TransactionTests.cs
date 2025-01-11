using Xunit.Abstractions;

namespace JustyBase.NetezzaDriver.Tests;

public class TransactionTests
{
    private static readonly string _password = Environment.GetEnvironmentVariable("NZ_DEV_PASSWORD") ?? throw new InvalidOperationException("Environment variable NZ_PASSWORD is not set.");

    [Fact]
    public void BasicTransactionsTests()
    {
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA");
        connection.Open();
        using var cursor = connection.CreateCommand();

        connection.AutoCommit = false; // autocommit is on by default. It can be turned off by using the autocommit property of the connection.

        cursor.CommandText = "DROP TABLE T2 IF EXISTS";
        cursor.ExecuteNonQuery();
        cursor.CommandText = "create table t2(c1 numeric (10,5), c2 varchar(10),c3 nchar(5))";
        cursor.ExecuteNonQuery();
        cursor.CommandText = "insert into t2 values (123.54,'xcfd','xyz')";
        cursor.ExecuteNonQuery();
        connection.Rollback();
        cursor.CommandText = "DROP TABLE T5 IF EXISTS";
        cursor.ExecuteNonQuery();
        cursor.CommandText = "create table t5(c1 numeric (10,5), c2 varchar(10),c3 nchar(5))";
        cursor.ExecuteNonQuery();
        cursor.CommandText = "insert into t5 values (123.54,'xcfd','xyz')";
        cursor.ExecuteNonQuery();
        connection.Commit();

        cursor.CommandText = "SELECT * FROM T2";
        Assert.Throws<NetezzaException>(() => cursor.ExecuteNonQuery());
        try
        {
            cursor.CommandText = "SELECT * FROM T5";
            cursor.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            Assert.Fail( $"Expected no exception, but got: {ex.Message}");
        }

    }


}
