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
    public void NumericScaleTest()
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
                    cmd.CommandText = $"SELECT 0::NUMERIC({precision},{scale}) {from}";
                    var reader = cmd.ExecuteReader();
                    var st = reader.GetSchemaTable();
                    var numericScale = (int)(st!.Rows[0]["NumericScale"]);

                    //Debug.WriteLine($"{connection.TypeModifierBinary(0)} NUMERIC({precision},{j})");
                    Assert.Equal(scale, numericScale);
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
//typeModyfier -> leads to the following scales
//010 001 -> 1
//010 010 -> 2
//010 011 -> 3
//010 100 -> 4
//010 101 -> 5
//010 110 -> 6
//010 111 -> 7
//011 000 -> 8
//011 001 -> 9
//011 010 -> 10
//011 011 -> 11
//011 100 -> 12
//011 101 -> 13
//011 110 -> 14
//011 111 -> 15
//100 000 -> 16
//100 001 -> 17
//…
//110 011 -> 35
//110 100 -> 36
//110 101 -> 37
//110 110 -> 38

// -> 
//X Y  
// X
//010 = 0 (2)
//011 = 8 (3)
//100 = 16 (4)
//110 = 32 (6)

//Y
//001 = 1 (1)
//010 = 2 (2)
//011 = 3 (3)
//....
// => scale = (X-2) * 8 + Y