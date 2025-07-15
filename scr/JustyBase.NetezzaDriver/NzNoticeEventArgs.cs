using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JustyBase.NetezzaDriver;

public sealed partial class NzNoticeEventArgs : System.EventArgs
{
    public NzNoticeEventArgs(string message)
    {
        Message = message;
    }
    public string Message { get; init; }
    public override string ToString() => Message;
}
