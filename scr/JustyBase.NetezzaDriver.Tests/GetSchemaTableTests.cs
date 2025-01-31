using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JustyBase.NetezzaDriver.Tests;

public class GetSchemaTableTests
{
    [Fact]
    public void NumericPrecisionScaleTest()
    {
        string _password = Environment.GetEnvironmentVariable("NZ_DEV_PASSWORD") ?? throw new InvalidOperationException("Environment variable NZ_PASSWORD is not set.");
        using NzConnection connection = new NzConnection("admin", _password, "linux.local", "JUST_DATA");
        connection.Open();
        connection.CommandTimeout = TimeSpan.FromSeconds(0);
        using var cmd = connection.CreateCommand();

        string[] froms = ["", "FROM JUST_DATA..DIMDATE LIMIT 1"];

        foreach (var from in froms)
        {
            for (int precision = 1; precision <= 38; precision++)
            {
                for (int scale = 0; scale <= precision && scale <= 28; scale++)
                {
                    cmd.CommandText = $"SELECT 0::NUMERIC({precision},{scale}) AS COL_XYZ {from}";
                    var reader = cmd.ExecuteReader();
                    var st = reader.GetSchemaTable();
                    var numericScale = (int)(st!.Rows[0]["NumericScale"]);
                    var numericPrecision = (int)(st!.Rows[0]["NumericPrecision"]);
                    var columnName = (string)(st!.Rows[0]["ColumnName"]);
                    var columnOrdinal = (int)(st!.Rows[0]["ColumnOrdinal"]);

                    Assert.Equal("COL_XYZ", columnName);
                    Assert.Equal(1, columnOrdinal);
                    
                    //Debug.WriteLine($"{connection.TypeModifierBinary(0)} NUMERIC({precision},{scale})");
                    Assert.Equal(scale, numericScale);
                    Assert.Equal(precision, numericPrecision);
                    do
                    {
                        Console.WriteLine(reader.HasRows);
                        while (reader.Read())
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                var o = reader.GetValue(i);
                            }
                        }
                    } while (reader.NextResult());
                }
            }
        }
    }
}
