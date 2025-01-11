

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JustyBase.NetezzaDriver.Tests;

public class InvalidSqlTests
{
    private static readonly string _password = Environment.GetEnvironmentVariable("NZ_DEV_PASSWORD") ?? throw new InvalidOperationException("Environment variable NZ_PASSWORD is not set.");

    [Fact]
    private void ReaderShouldThrow()
    {
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA", 5480, logger: null);
        connection.Open();
        connection.CommandTimeout = TimeSpan.FromSeconds(120);
        using var cursor = connection.CreateCommand();
        cursor.CommandText = "SELECT 1,,2;SELECT 1,2";
        Assert.Throws<NetezzaException>(() => cursor.ExecuteReader());
    }

    [Fact]
    private void ExecuteNonQueryShouldThrow()
    {
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA", 5480, logger: null);
        connection.Open();
        connection.CommandTimeout = TimeSpan.FromSeconds(120);
        using var cursor = connection.CreateCommand();
        cursor.CommandText = "SELECT 1,,2;SELECT 1,2";
        Assert.Throws<NetezzaException>(() => cursor.ExecuteNonQuery());
    }

    [Fact]
    private void ExecuteScalarShouldThrow()
    {
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA", 5480, logger: null);
        connection.Open();
        connection.CommandTimeout = TimeSpan.FromSeconds(120);
        using var cursor = connection.CreateCommand();
        cursor.CommandText = "SELECT 1,,2;SELECT 1,2";
        Assert.Throws<NetezzaException>(() => cursor.ExecuteScalar());
    }
}

