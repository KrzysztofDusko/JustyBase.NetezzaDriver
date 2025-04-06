using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JustyBase.NetezzaDriver;

public sealed class NzConnectionStringBuilder
{
    public string Host { get; set; } = string.Empty;
    public string Database { get; set; } = string.Empty;
    public string UserName { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public int Port { get; set; } = 5480;
    public int Timeout { get; set; } = 0; // 0 means no timeout
    //public bool UseSSL { get; set; } = false;
    //public bool UseSSLOnly { get; set; } = false;
    //public string SSLCertFilePath { get; set; } = string.Empty;
    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append($"Host={Host};");
        sb.Append($"Database={Database};");
        sb.Append($"User={UserName};");
        sb.Append($"Password={Password};");
        sb.Append($"Port={Port};");
        sb.Append($"Timeout={Timeout};");
        //sb.Append($"UseSSL={UseSSL};");
        //sb.Append($"UseSSLOnly={UseSSLOnly};");
        //if (!string.IsNullOrEmpty(SSLCertFilePath))
        //    sb.Append($"SSLCertFilePath={SSLCertFilePath};");
        return sb.ToString();
    }
    public string ConnectionString => ToString();
}
