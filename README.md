# JustyBase.NetezzaDriver 
JustyBase.NetezzaDriver is a .NET library for interacting with IBM Netezza Performance Server databases. It provides a set of classes and methods to facilitate database connections, command execution, and data retrieval.
Code is is based on [nzpy](https://github.com/IBM/nzpy) and [npgsql](https://github.com/npgsql/npgsql)


## Features
* Connect to Netezza databases using NzConnection.
* Execute SQL commands and queries with NzCommand.
* Read data using NzDataReader.
* Support for various Netezza data types.
* Secure connections with SSL/TLS.

## Requirements
* .NET 8 or .NET 9
* C# 12.0
## Installation
JustyBase.NetezzaDriver is available as a NuGet package. You can install it using the following command:
```bash
dotnet add package JustyBase.NetezzaDriver
```

## Usage
### Connecting to a Database
To connect to a Netezza database, create an instance of NzConnection and open the connection:
```c#
using JustyBase.NetezzaDriver;

var connection = new NzConnection("username", "password", "host", "database");
connection.Open();
```

### Executing a Command

```c#
var command = new NzCommand(connection)
{
    CommandText = "SELECT * FROM my_table"
};

using var reader = command.ExecuteReader();
while (reader.Read())
{
    for (int i = 0; i < reader.FieldCount; i++)
    {
        var value = reader.GetValue(i);
        Console.WriteLine(value);
    }
}

```
### Handling Transactions
To handle transactions, use the BeginTransaction, Commit, and Rollback methods:

```c#
        using NzConnection connection = new NzConnection("username", "password", "host", "database");
        connection.Open();
        using var nzCommand = connection.CreateCommand();

        connection.AutoCommit = false; // autocommit is on by default. It can be turned off by using the autocommit property of the connection.

        nzCommand.CommandText = "DROP TABLE T2 IF EXISTS";
        nzCommand.ExecuteNonQuery();
        nzCommand.CommandText = "create table t2(c1 numeric (10,5), c2 varchar(10),c3 nchar(5))";
        nzCommand.ExecuteNonQuery();
        nzCommand.CommandText = "insert into t2 values (123.54,'xcfd','xyz')";
        nzCommand.ExecuteNonQuery();
        connection.Rollback();
        nzCommand.CommandText = "DROP TABLE T5 IF EXISTS";
        nzCommand.ExecuteNonQuery();
        nzCommand.CommandText = "create table t5(c1 numeric (10,5), c2 varchar(10),c3 nchar(5))";
        nzCommand.ExecuteNonQuery();
        nzCommand.CommandText = "insert into t5 values (123.54,'xcfd','xyz')";
        nzCommand.ExecuteNonQuery();
        connection.Commit();

        nzCommand.CommandText = "SELECT * FROM T2";
        Assert.Throws<NetezzaException>(() => nzCommand.ExecuteNonQuery());
        try
        {
            nzCommand.CommandText = "SELECT * FROM T5";
            nzCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            Assert.Fail( $"Expected no exception, but got: {ex.Message}");
        }
```
### Secure Connections
⚠️ **Warning** Aboring the secure connection is not implemented yet. This feature in not well tested. PR's are welcome.

To establish a secure connection, provide the SSL certificate file path when creating the NzConnection instance:
```c#
var connection = new NzConnection("username", "password", "host", "database", securityLevel: 3, sslCerFilePath: "path/to/certificate.pem");
connection.Open();
```
### Testing
To run the tests, use the following command:
```bash
dotnet test
```

## License
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.

```plaintext
Copyright: 2025 Krzysztof Duśko
Copyright: 2019-2020 IBM, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## Contact
For questions or support, please open an issue on GitHub.