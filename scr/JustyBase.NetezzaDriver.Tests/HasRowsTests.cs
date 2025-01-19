﻿namespace JustyBase.NetezzaDriver.Tests;

public class HasRowsTests
{

    [Fact]
    public void Test1()
    {
        Assert.Equal([false], Helper("delete from FACTPRODUCTINVENTORY where 1=2;"));
    }

    [Fact]
    public void Test2()
    {
        string query = "SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY DD ORDER BY ROWID LIMIT 0;" +
                           "SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY DD ORDER BY ROWID LIMIT 1;" +
                           "SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY DD ORDER BY ROWID LIMIT 0;" +
                           "SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY DD ORDER BY ROWID LIMIT 1;";
        Assert.Equal([false,true,false,true], Helper(query));
    }

    [Fact]
    public void Test3()
    {
        string query = "SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY DD ORDER BY ROWID LIMIT 0;" +
            "SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY DD ORDER BY ROWID LIMIT 1;" +
            "SELECT * FROM JUST_DATA..FACTPRODUCTINVENTORY DD ORDER BY ROWID LIMIT 0;" +
            "delete from FACTPRODUCTINVENTORY where 1=2;" +
            "delete from FACTPRODUCTINVENTORY where 1=2;" +
            "SELECT 11 FROM JUST_DATA..FACTPRODUCTINVENTORY DD ORDER BY ROWID LIMIT 10";
        Assert.Equal([false, true, false, true], Helper(query));
    }

    [Fact]
    public void Test4()
    {
        string query = "delete from FACTPRODUCTINVENTORY where 1=2;delete from FACTPRODUCTINVENTORY where 1=2;select 10";
        Assert.Equal([true], Helper(query));
    }

    private List<bool> Helper(string query)
    {
        string _password = Environment.GetEnvironmentVariable("NZ_DEV_PASSWORD") ?? throw new InvalidOperationException("Environment variable NZ_PASSWORD is not set.");
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA");
        connection.Open();
        connection.CommandTimeout = TimeSpan.FromSeconds(0);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = query;

        List<bool> hasRows = new List<bool>();
        var reader = cmd.ExecuteReader();
        do
        {
            hasRows.Add(reader.HasRows);
            while (reader.Read()) ;
        } while (reader.NextResult());

        return hasRows;
    }


}
