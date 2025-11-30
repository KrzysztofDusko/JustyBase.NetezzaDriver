using System;
using System.Collections.Generic;
using System.Text;

namespace JustyBase.NetezzaDriver.Tests;

public static class Config
{
    public const string Host = "172.23.174.131";
    public const int Port = 5480;
    public const string DbName = "JUST_DATA";
    public const string UserName = "admin";
    public static string Password = Environment.GetEnvironmentVariable("NZ_DEV_PASSWORD") ?? throw new InvalidOperationException("Environment variable NZ_PASSWORD is not set.");
}
