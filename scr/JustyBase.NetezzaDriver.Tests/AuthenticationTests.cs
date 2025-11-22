using Xunit;
using JustyBase.NetezzaDriver;

namespace JustyBase.NetezzaDriver.Tests;

public class AuthenticationTests
{
    private const string _host = "linux.local";
    private const string _dbName = "JUST_DATA";
    private const string _userName = "admin";
    private const string _invalidPassword = "WrongPassword123!";
    private const int _port = 5480;

    [Fact]
    public void Open_WithInvalidPassword_ThrowsNetezzaException()
    {
        // Arrange
        var connectionString = $"Host={_host};Database={_dbName};User={_userName};Password={_invalidPassword};Port={_port};Timeout=5";
        using var connection = new NzConnection(connectionString);

        // Act & Assert
        var exception = Assert.Throws<NetezzaException>(() => connection.Open());
        // The driver currently throws "Error in handshake" on auth failure
        Assert.Contains("Password authentication failed", exception.Message);
    }
}
