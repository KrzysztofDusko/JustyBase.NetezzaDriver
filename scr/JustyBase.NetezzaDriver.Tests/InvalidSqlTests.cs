namespace JustyBase.NetezzaDriver.Tests;

public class InvalidSqlTests
{
    private static readonly string _password = Environment.GetEnvironmentVariable("NZ_DEV_PASSWORD") ?? throw new InvalidOperationException("Environment variable NZ_PASSWORD is not set.");

    [Fact]
    private void ReaderShouldThrow()
    {
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA");
        connection.Open();
        connection.CommandTimeout = TimeSpan.FromSeconds(120);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1,,2;SELECT 1,2";
        Assert.Throws<NetezzaException>(() => command.ExecuteReader());
    }

    [Fact]
    private void ExecuteNonQueryShouldThrow()
    {
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA");
        connection.Open();
        connection.CommandTimeout = TimeSpan.FromSeconds(120);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1,,2;SELECT 1,2";
        Assert.Throws<NetezzaException>(() => command.ExecuteNonQuery());
    }

    [Fact]
    private void ExecuteScalarShouldThrow()
    {
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA");
        connection.Open();
        connection.CommandTimeout = TimeSpan.FromSeconds(120);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1,,2;SELECT 1,2";
        Assert.Throws<NetezzaException>(() => command.ExecuteScalar());
    }
}

