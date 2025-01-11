using JustyBase.NetezzaDriver;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestProject;

public class ExternalTableTests
{
    private static readonly string _password = Environment.GetEnvironmentVariable("NZ_DEV_PASSWORD") ?? throw new InvalidOperationException("Environment variable NZ_PASSWORD is not set.");

    [Fact]
    public void TestExternalTable()
    {
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA", 5480, logger: null);
        connection.Open();
        using NzCommand cursor = (NzCommand)connection.CreateCommand();
        cursor.CommandText = "SELECT TABLENAME FROM JUST_DATA.._V_TABLE WHERE OBJTYPE = 'TABLE' AND TABLENAME NOT LIKE '%EXTERNAL' ORDER BY CREATEDATE ASC";
        using var reader = cursor.ExecuteReader();
        var tableNames = new List<string>();
        while (reader.Read())
        {
            tableNames.Add(reader.GetString(0));
        }

        Assert.True(tableNames.Count > 10);
        Assert.True(tableNames.Count < 100);
        foreach (var tn in tableNames.Take(3))
        {
            Assert.NotNull(tn);
            TestOneTable(cursor, tn);
        }
    }

    private static void TestOneTable(NzCommand cursor, string tablename)
    {
        var externalPath = $"E:\\{tablename}.dat";
        var tablenameOrg = $"JUST_DATA..{tablename}";
        var tablenameNew = $"{tablenameOrg}_FROM_EXTERNAL";

        cursor.CommandText = $"DROP TABLE {tablenameNew} IF EXISTS";
        cursor.ExecuteNonQuery();
        cursor.CommandText = $"DROP TABLE ET_TEMP IF EXISTS";
        cursor.ExecuteNonQuery();
        cursor.CommandText = $"create external table ET_TEMP '{externalPath}' using ( remotesource 'python' delimiter '|') as select * from {tablenameOrg}";
        cursor.ExecuteNonQuery();
        cursor.CommandText = $"DROP TABLE {tablenameNew} IF EXISTS";
        cursor.ExecuteNonQuery();
        cursor.CommandText = $"CREATE TABLE {tablenameNew} AS (SELECT * FROM {tablenameOrg} WHERE 1=2)";
        cursor.ExecuteNonQuery();
        cursor.CommandText = $"INSERT INTO {tablenameNew}  SELECT * FROM EXTERNAL '{externalPath}' " +
            @"using ( remotesource 'python' delimiter '|' socketbufsize 8388608 ctrlchars 'yes'  encoding 'internal' timeroundnanos 'yes' crinstring 'off' logdir E:\logs\)";
        cursor.ExecuteNonQuery();

        cursor.CommandText = $"SELECT count(1) FROM {tablenameOrg}";
        using var rd1 = cursor.ExecuteReader();
        rd1.Read();
        long cnt_org = rd1.GetInt64(0);
        cursor.CommandText = $"SELECT count(1) FROM {tablenameNew}";
        using var rd2 = cursor.ExecuteReader();
        rd2.Read();
        long cnt_new = rd2.GetInt64(0);

        Assert.Equal(cnt_org, cnt_new);
        cursor.CommandText = $"SELECT *  FROM {tablenameNew} minus SELECT *  FROM {tablenameOrg}";
        using var rd3 = cursor.ExecuteReader();
        int cnt = 0;
        while (rd3.Read())
        {
            cnt++;
        }

        Assert.Equal(0, cnt);
    }
}
