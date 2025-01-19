using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JustyBase.NetezzaDriver.Logging;

public interface ISimpleNzLogger
{
    void LogDebug(string? message, params object?[] args) { }
    void LogError(string? message, params object?[] args) { }
    void LogInformation(string? message, params object?[] args) { }
    void LogWarning(string? message, params object?[] args) { }
}

public sealed class SimpleNzLogger : ISimpleNzLogger
{
    public void LogDebug(string? message, params object?[] args) { }
    public void LogError(string? message, params object?[] args) { }
    public void LogInformation(string? message, params object?[] args) { }
    public void LogWarning(string? message, params object?[] args) { }
}
