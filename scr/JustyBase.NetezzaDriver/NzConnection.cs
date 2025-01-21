using JustyBase.NetezzaDriver.AbortQuery;
using JustyBase.NetezzaDriver.Logging;
using JustyBase.NetezzaDriver.StringPool;
using JustyBase.NetezzaDriver.TypeConvertions;
using JustyBase.NetezzaDriver.Utility;
using System.Buffers;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Net.Sockets;
using System.Text;

namespace JustyBase.NetezzaDriver;

public sealed class NzConnection : DbConnection
{
    private string? _error;
 
    //private Queue<object> _parameterStatuses = new Queue<object>(100);
    //private readonly int _maxPreparedStatements = 1000;

    //????
    //private Dictionary<object, Dictionary<object, Dictionary<object, Dictionary<object, Dictionary<object, object>>>>> _caches = new();
    private int _commandNumber = -1;

    public bool InTransaction { get; set; } = false;//TODO
    public bool AutoCommit { get;set; } = true;

    /// <summary>
    /// The physical connection socket to the backend.
    /// </summary>
    Socket _socket = default!;

    /// <summary>
    /// The physical connection stream to the backend, without anything on top.
    /// </summary>
    //NetworkStream _baseStream = default!;
    public bool IsBaseStreamNull => _stream == null;//??

    /// <summary>
    /// The physical connection stream to the backend, layered with an SSL/TTLS stream if in secure mode.
    /// this should be used mailny for reading and writing?
    /// </summary>
    Stream _stream = default!;

    private readonly List<string> _commandsWithCount = ["INSERT", "DELETE", "UPDATE"];
    private readonly ISimpleNzLogger? _logger = null!;
    private readonly bool _tcpKeepAlive = true;

    private BackendKeyDataMessage _backendKeyData = null!;
    public int? Pid => _backendKeyData.BackendProcessId;

    private readonly string _database;
    private readonly string _user;
    private readonly string _password;
    private readonly string _host;
    private readonly int _port;

    public const int NzTypeRecAddr = 1;
    public const int NzTypeDouble = 2;
    public const int NzTypeInt = 3;
    public const int NzTypeFloat = 4;
    public const int NzTypeMoney = 5;
    public const int NzTypeDate = 6;
    public const int NzTypeNumeric = 7;
    public const int NzTypeTime = 8;
    public const int NzTypeTimestamp = 9;
    public const int NzTypeInterval = 10;
    public const int NzTypeTimeTz = 11;
    public const int NzTypeBool = 12;
    public const int NzTypeInt1 = 13;
    public const int NzTypeBinary = 14;
    public const int NzTypeChar = 15;
    public const int NzTypeVarChar = 16;
    public const int NzDEPR_Text = 17; // OBSOLETE 3.0: BLAST Era Large 'text' Object
    public const int NzTypeUnknown = 18; // corresponds to PG UNKNOWNOID data type - an untyped string literal
    public const int NzTypeInt2 = 19;
    public const int NzTypeInt8 = 20;
    public const int NzTypeVarFixedChar = 21;
    public const int NzTypeGeometry = 22;
    public const int NzTypeVarBinary = 23;
    public const int NzDEPR_Blob = 24; // OBSOLETE 3.0: BLAST Era Large 'binary' Object
    public const int NzTypeNChar = 25;
    public const int NzTypeNVarChar = 26;
    public const int NzDEPR_NText = 27; // OBSOLETE 3.0: BLAST Era Large 'nchar text' Object
                                        // skip 28
                                        // skip 29
    public const int NzTypeJson = 30;
    public const int NzTypeJsonb = 31;
    public const int NzTypeJsonpath = 32;
    public const int NzTypeLastEntry = 33;
    public const int NzTypeIntvsAbsTimeFIX = 39;//https://github.com/IBM/nzpy/issues/61

    public NzConnection(string user, string password, string host, string database,
        int port = 5480, int securityLevel = 0, string? sslCerFilePath = null, ISimpleNzLogger? logger = null)
    {
        _logger = logger;
        _securityLevel = securityLevel;
        _sslCerFilePath = sslCerFilePath;
        _database = database;
        _user = user;
        _password = password;
        _host = host;
        _port = port;
        _tmp_buffer = ArrayPool<byte>.Shared.Rent(4096);
    }


    public TimeSpan ConnectionTimeoutDuration { get; init; } =  TimeSpan.FromSeconds(15);

    public override int ConnectionTimeout => (int)ConnectionTimeoutDuration.TotalSeconds;

    private Stream Initialize(string host, int port)
    {
        try
        {
            if (host is not null)
            {
                _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            }
            else
            {
                throw new NetezzaException("one of host or unix_sock must be provided");
            }

            if (host is not null)
            {
                var beginConnect = _socket.BeginConnect(host, port, null, null);
                if (!beginConnect.AsyncWaitHandle.WaitOne(ConnectionTimeoutDuration, true))
                {
                    _socket.Close();
                    throw new NetezzaException("Connection timeout");
                }
                _socket.EndConnect(beginConnect);
                //_socket.Connect(host, port);
            }
            var baseStream = new NetworkStream(_socket, ownsSocket: true);
            var tmpStream = new BufferedStream(baseStream);

            if (_tcpKeepAlive)
            {
                _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            }
            return tmpStream;
        }
        catch (Exception ex)
        {
            _socket.Close();
            throw new InterfaceException("communication error", ex);
        }
    }

#if NET9_0_OR_GREATER
    private static readonly Lock _cancelLock = new ();
#else
    private static readonly object _cancelLock = new ();
#endif
    //TODO SSL CASE..
    public void CancelQuery()
    {
        lock (_cancelLock)
        {
            int pid = _backendKeyData.BackendProcessId;
            int secretKey = _backendKeyData.BackendSecretKey;
            var socketX = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            //socketX.Connect(_host, _port);
            var beginConnect = socketX.BeginConnect(_host, _port, null, null);
            if (!beginConnect.AsyncWaitHandle.WaitOne(ConnectionTimeoutDuration, true))
            {
                socketX.Close();
                throw new NetezzaException("Connection timeout");
            }
            socketX.EndConnect(beginConnect);

            var baseStream = new NetworkStream(socketX, ownsSocket: true);
            var stream2 = new BufferedStream(baseStream);

            Canceling.WriteCancelRequest(stream2, pid, secretKey);
            stream2.Flush();
            var count = stream2.ReadByte();
            stream2.Dispose();
            //Error = "Query canceled";
            //Status = Core.CONN_CANCELLED;
        }
    }

    //public void Terminate()
    //{
    //    const int len = sizeof(byte) +  // Message code
    //            sizeof(int);    // Length
    //    PGUtil.WriteInt32_I(_stream, len);
    //    _stream.Write([FrontendMessageCode.Terminate]);
    //    PGUtil.WriteInt32_I(_stream, len - 1);
    //    _stream.Flush();
    //}

    /// <summary>
    /// Sends initial connection queries to setup database session
    /// </summary>
    private bool ConnSendQuery(string dateStyle = "ISO")
    {
        if (!Execute(_nzCommand, "set nz_encoding to 'utf8'"))
            return false;

        // Set the Datestyle to the format the driver expects
        string query = dateStyle switch
        {
            "MDY" => "set DateStyle to 'US'",
            "DMY" => "set DateStyle to 'EUROPEAN'",
            _ => "set DateStyle to 'ISO'"
        };

        if (!Execute(_nzCommand, query))
            return false;

        var systemArch = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture;
        string procArch = systemArch == System.Runtime.InteropServices.Architecture.X64 ? "AMD64" : "other";

        string clientInfo = $@"select version(), 
        'Netezza Python Client Version {NzConnectionHelpers.NZPY_CLIENT_VERSION}', 
        '{procArch}',
        'OS Platform: {Environment.OSVersion}',
        'OS Username: {Environment.UserName}'";

        _nzCommand.CommandText = clientInfo;
        using var rdr = _nzCommand.ExecuteReader();

        while (rdr.Read())
        {
            var row = new object[5];
            rdr.GetValues(row);
            _serverVersion = (row[0] as string) ?? "no version info";
            _logger?.LogDebug("Version info: {row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}",
                row[0], row[1], row[2], row[3], row[4]);
        }


        if (!Execute(_nzCommand, $"SET CLIENT_VERSION = '{NzConnectionHelpers.NZPY_CLIENT_VERSION}'"))
            return false;


        _nzCommand.CommandText = @"select ascii(' ') as space, encoding as ccsid from _v_database where objid = current_db";
        using var rdr2 = _nzCommand.ExecuteReader();
        while (rdr2.Read())
        {
            var row = new object[2];
            rdr2.GetValues(row);
            _logger?.LogDebug("Space: {row[0]}, CCSID: {row[1]}", row[0], row[1]);
        }

        _nzCommand.CommandText = @"select feature from _v_odbc_feature where spec_level = '3.5'";
        using var rdr3 = _nzCommand.ExecuteReader();
        while (rdr3.Read())
        {
            var row = new object[1];
            rdr3.GetValues(row);
            _logger?.LogDebug("Feature: {row[0]}", row[0]);
        }
        _nzCommand.CommandText = "select identifier_case, current_catalog, current_user";

        using var rdr4 = _nzCommand.ExecuteReader();
        while (rdr4.Read())
        {
            var row = new object[3];
            rdr4.GetValues(row);
            _logger?.LogDebug("Case: {row[0]}, Catalog: {row[1]}, User: {row[2]}", row[0], row[1], row[2]);
        }

        return true;
    }

    private readonly Dictionary<string, string>? _pgOptions = null;

    private readonly int _securityLevel = 0;
    private readonly string? _sslCerFilePath;

    private bool _disposed = false;
    protected override void Dispose(bool disposing)
    {
        if (_disposed)
            return;
        if (disposing)
            Close();
        _disposed = true;
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        await CloseAsync().ConfigureAwait(false);
        _disposed = true;
    }

    public void Commit()
    {
        Execute(this._nzCommand, "commit");
    }

    public void Rollback()
    {
        if (this.InTransaction)
        {
            Execute(this._nzCommand, "rollback");
            InTransaction = false;
        }
    }

    private void PreExecution(NzCommand nzCommand, string query)
    {
        _error = null;
        nzCommand._recordsAffected = -1;
        nzCommand.NewPreparedStatement = new PreparedStatement();
        nzCommand.NewPreparedStatement.Sql = query;
        //if (State == ConnectionState.Executing)
        if (State != ConnectionState.Connecting)
        {
            PGUtil.Skip4Bytes(_stream!);
        }

        List<byte> buf;

        if (_commandNumber != -1)
        {
            _commandNumber += 1;
            buf = new List<byte> { (byte)'P' };
            buf.AddRange(Core.IPack(_commandNumber));
        }
        else
        {
            buf = [(byte)'P', 0xFF, 0xFF, 0xFF, 0xFF];
        }

        if (_commandNumber > 100000)
        {
            _commandNumber = 1;
        }

        if (query != null)
        {
            var queryBytes = Encoding.UTF8.GetBytes(query);
            buf.AddRange(queryBytes);
            buf.Add((byte)0);
        }
        _stream.Write(buf.ToArray());
        _stream.Flush();

        _logger?.LogDebug("Buffer sent to nps: {Buffer}", NzConnectionHelpers.ClientEncoding.GetString(buf.ToArray()));

        _state = ConnectionState.Executing;
    }

    private byte[] Read(int length, byte[]? buffer = null)
    {
        byte[] buf = buffer ?? new byte[length];
        _stream.ReadExactly(buf, 0, length);
        return buf;
    }
    private void WriteSpan(Span<byte> buf)
    {
        _stream.Write(buf);
    }

    private void Flush()
    {
        _stream.Flush();
    }

    public event Action<string>? NoticeReceived;

    private void OnNoticeReceived(string notice)
    {
        if (notice.StartsWith("NOTICE:"))
        {
            notice = notice["NOTICE:".Length..];
        }
        notice = notice.Trim().TrimEnd('\x00');
        NoticeReceived?.Invoke(notice);
    }

    public TimeSpan CommandTimeout { get; set; } =  TimeSpan.FromSeconds(60);
    [AllowNull]
    public override string ConnectionString { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

    public override string Database => _database;

    public override string DataSource => throw new NotImplementedException();

    private string _serverVersion = "";
    public override string ServerVersion => _serverVersion;


    private ConnectionState _state = ConnectionState.Closed;
    public override ConnectionState State => _state;

    internal void SetState(ConnectionState state)
    {
        _state = state;
    }

    private DbosTupleDesc _tupdesc = null!;

    private void HandleTimeout()
    {
        double microsToWait = CommandTimeout.TotalMicroseconds;
        bool pollresult = true;
        while (microsToWait > 0.5)
        {
            int toWait;
            if (microsToWait < int.MaxValue)
            {
                toWait = (int)microsToWait;
                microsToWait = 0;
            }
            else
            {
                toWait = int.MaxValue;
                microsToWait -= toWait;
            }
            pollresult = _socket.Poll(toWait, SelectMode.SelectRead);
            if (!pollresult)
            {
                break;
            }
        }
        if (!pollresult)
        {
            CancelQuery();
            _error = "Command timeout";
        }
    }


    public bool Execute(NzCommand nzCommand, string query)
    {
        PreExecution(nzCommand, query);
        HandleTimeout();
        _nextRelatedFileStream = null!;

        while (DoNextStep(nzCommand)) ;
        var response = true;

        if (_error != null)
        {
            throw new NetezzaException(_error);
        }

        return response;
    }

    public NzDataReader ExecuteReader(NzCommand nzCommand, string query)
    {
        PreExecution(nzCommand, query);
        HandleTimeout();
        _nextRelatedFileStream = null!;
        var rdr =  new NzDataReader(nzCommand);
        if (_error != null)
        {
            throw new NetezzaException(_error);
        }
        return rdr;
    }

    private int _lastResponse = -1;

    private FileStream _nextRelatedFileStream = null!;

    internal bool NewRowReceived()
    {
        return _lastResponse == (byte)BackendMessageCode.RowStandard || _lastResponse == (byte)BackendMessageCode.DataRow;
    }
    internal bool NewRowDescriptionReceived()
    {
        return _lastResponse == (byte)BackendMessageCode.RowDescription;
    }
    internal bool NewRowDescriptionStandardReceived()
    {
        return _lastResponse == (byte)BackendMessageCode.RowDescriptionStandard;
    }

    internal bool IsCommandComplete()
    {
        return _lastResponse == (byte)BackendMessageCode.CommandComplete;
    }

    internal bool DoNextStep(NzCommand nzCommand)
    {
        if (_shouldReadByte)
        {
            ReadNextResponseByte();
        }
        var res = IntepretReturnedByte(nzCommand);
        return res;
    }

    internal void ReadNextResponseByte()
    {
        _lastResponse = _stream.ReadByte();
        _shouldReadByte = false;
    }
    private bool _shouldReadByte = true;

    private bool IntepretReturnedByte(NzCommand nzCommand)
    {
        _shouldReadByte = true;
        _logger?.LogDebug("Backend response: {Response}", (char)_lastResponse);
        PGUtil.Skip4Bytes(_stream);

        if (_lastResponse == (byte)BackendMessageCode.CommandComplete)
        {
            // portal query command, no tuples returned
            int length = PGUtil.ReadInt32(_stream);
            RegenerateBuffer(length);
            var data = Read(length, _tmp_buffer);
            HandleCommandComplete(data, length,  nzCommand);
            //returnet data informs about command type (SELECT/SET VARIABLE/...)
            _logger?.LogDebug("Response received from backend: {Data}", Encoding.UTF8.GetString(data, 0, length));
        }
        else if (_lastResponse == (byte)BackendMessageCode.ReadyForQuery)
        {
            return false;
        }
        else if (_lastResponse == (byte)'L')
        {
            return false;
        }
        else if (_lastResponse == (byte)'0')
        {
            //return true;
        }
        else if (_lastResponse == (byte)'A')
        {
            //return true;
        }
        else if (_lastResponse == (byte)'P')//80
        {
            int length = PGUtil.ReadInt32(_stream);
            RegenerateBuffer(length);
            var data = Read(length, _tmp_buffer);
            _logger?.LogDebug("Response received from backend: {Data}", Encoding.UTF8.GetString(data, 0, length));
            //doContinue = true;
        }
        else if (_lastResponse == (byte)BackendMessageCode.ErrorResponse)
        {
            int length = PGUtil.ReadInt32(_stream);
            RegenerateBuffer(length);
            var data = Read(length, _tmp_buffer);
            _error = Encoding.UTF8.GetString(data,0,length);
            _logger?.LogDebug("Response received from backend: {_error}", _error);
            //doContinue = true;
        }
        //this STARTS (after 'P') single rowset
        else if (_lastResponse == (byte)BackendMessageCode.RowDescription)
        {
            int length = PGUtil.ReadInt32(_stream);
            nzCommand.NewPreparedStatement ??= new PreparedStatement();
            RegenerateBuffer(length);
            var data = Read(length, _tmp_buffer);
            NzConnection.HandleRowDescription(data, nzCommand);
            // We've got row_desc that allows us to identify what we're going to get back from this statement.
            //nzCommand.NewPreparedStatement.input_funcs = nzCommand.NewPreparedStatement!.Description!.GetFuncArray;
        }
        else if (_lastResponse == (byte)BackendMessageCode.DataRow)//read rows in schema/system queries - hot path
        {
            int length = PGUtil.ReadInt32(_stream);
            RegenerateBuffer(length);
            var data = Read(length, _tmp_buffer);
            HandleDataRow(data, nzCommand); 
        }
        else if (_lastResponse == (byte)BackendMessageCode.RowDescriptionStandard)// metadata for standard query, occurs after BackendMessageCode.RowDescription
        {
            int length = PGUtil.ReadInt32(_stream);
            _tupdesc = new DbosTupleDesc();
            RegenerateBuffer(length);
            var data = Read(length, _tmp_buffer);
            ResGetDbosColumnDescriptions(data);
            //doContinue = true;
        }
        else if (_lastResponse == (byte)BackendMessageCode.RowStandard)//!!!!!!, main hot path - read rows
        {
            ResReadDbosTuple(nzCommand);
            //Thread.Sleep(50);
            //doContinue = true;
        }
        else if (_lastResponse == (byte)'u')
        {
            // unload - initialize application protocol
            // in ODBC, the first 10 bytes are utilized to populate clientVersion, formatType and bufSize
            // these are not needed in go lang, hence ignoring 10 bytes
            PGUtil.Skip4Bytes(_stream, 10);
            // Next 16 bytes are Reserved Bytes for future extension
            PGUtil.Skip4Bytes(_stream, 16);
                                           // Get the filename (specified in dataobject)
            int length = PGUtil.ReadInt32(_stream);
            var fnameBuf = Read(length);//this should be short
            var finaName = Encoding.UTF8.GetString(fnameBuf,0,length);
            try
            {
                _nextRelatedFileStream = new FileStream(finaName, FileMode.OpenOrCreate, FileAccess.Write);
                _logger?.LogDebug("Successfully opened file: {Filename}", finaName);
                // file open successfully, send status back to datawriter
                var buf = Core.IPack(0);
                WriteSpan(buf);
                Flush();
            }
            catch (Exception)
            {
                _logger?.LogWarning("Error while opening file");
            }
        }
        else if (_lastResponse == (byte)'U')
        {
            // handle unload data
            ReceiveAndWriteDataToExternal(_nextRelatedFileStream);
        }
        else if (_lastResponse == (byte)'l')
        {
            XferTable();
        }
        else if (_lastResponse == (byte)'x')
        {
            // handle Ext Tbl parser abort
            PGUtil.Skip4Bytes(_stream);
            _logger?.LogWarning("Error operation cancel");
        }
        else if (_lastResponse == (byte)'e')
        {
            int length = PGUtil.ReadInt32(_stream);
            string logDir = Encoding.UTF8.GetString(Read(length - 1));

            _stream.ReadByte();
            // ignore one byte as it is null character at the end of the string
            var filenameBuf = new List<byte> { Read(1)[0] };
            while (true)
            {
                var charByte = Read(1)[0];
                if (charByte == 0x00)
                {
                    break;
                }
                filenameBuf.Add(charByte);
            }

            string filename = Encoding.UTF8.GetString(filenameBuf.ToArray());
            int logType = PGUtil.ReadInt32(_stream);
            if (!GetFileFromBE(logDir, filename, logType))
            {
                _logger?.LogDebug("Error in writing file received from BE");
            }
            //doContinue = true;
        }
        else if (_lastResponse == (byte)BackendMessageCode.NoticeResponse)
        {
            int length = PGUtil.ReadInt32(_stream);
            RegenerateBuffer(length);
            var data = Read(length, _tmp_buffer);
            string notice = Encoding.UTF8.GetString(data[0..length]);
            OnNoticeReceived(notice);
            _logger?.LogDebug("Response received from backend: {Notice}", notice);
        }
        else if (_lastResponse == (byte)'I')
        {
            int length = PGUtil.ReadInt32(_stream);
            RegenerateBuffer(length);
            var data = Read(length, _tmp_buffer);
            string notice = Encoding.UTF8.GetString(data[0..length]);
            OnNoticeReceived(notice);
            _logger?.LogDebug("Response received from backend: {Notice}", notice);
            nzCommand.AddRow([]);
        }

        return true;
    }



    //(from file/pipe to Nz) = upload = import to database
    private void XferTable()
    {
        PGUtil.Skip4Bytes(_stream);
        int clientVersion = 1;

        byte charByte = Read(1)[0];
        
        var filenameBuf = new List<byte> { charByte };
        while (true)
        {
            charByte = Read(1)[0];
            if (charByte == 0x00)
            {
                break;
            }
            filenameBuf.Add(charByte);
        }

        string filename = NzConnectionHelpers.ClientEncoding.GetString(filenameBuf.ToArray());

        int hostVersion = PGUtil.ReadInt32(_stream);
        //Write(Core.IPack(clientVersion));
        PGUtil.WriteInt32(_stream, clientVersion);


        Flush();

        int format = PGUtil.ReadInt32(_stream);
        int blockSize = PGUtil.ReadInt32(_stream);
        _logger?.LogInformation("Format={Format} Block size={BlockSize} Host version={HostVersion}", format, blockSize, hostVersion);

        try
        {
            //if file is utf8 encoded, we can skip reencoding..
            using (var filehandle = new StreamReader(filename))
            {
                _logger?.LogInformation("Successfully opened External file to read: {Filename}", filename);
                char[] charsBuffer = ArrayPool<char>.Shared.Rent(blockSize);
                byte[] bytesBuffer = ArrayPool<byte>.Shared.Rent(4 * blockSize);//utf8 is max 4 bytes per char
                while (true)
                {
                    int charsReaded = filehandle.Read(charsBuffer, 0, blockSize);
                    if (charsReaded == 0)
                    {
                        break;
                    }

                    Span<byte> dataBytes = bytesBuffer.AsSpan(); 
                    int bytesCount = Encoding.UTF8.GetBytes(charsBuffer.AsSpan(0, charsReaded), dataBytes);
                    dataBytes = dataBytes[..bytesCount];

                    if (blockSize < dataBytes.Length)
                    {
                        int diff = dataBytes.Length - blockSize;

                        PGUtil.WriteInt32(_stream, Core.EXTAB_SOCK_DATA);
                        PGUtil.WriteInt32(_stream, blockSize);
                        WriteSpan(dataBytes[..blockSize]);
                        Flush();

                        PGUtil.WriteInt32(_stream, Core.EXTAB_SOCK_DATA);
                        PGUtil.WriteInt32(_stream, diff);
                        WriteSpan(dataBytes[blockSize..]);
                        Flush();
                    }
                    else
                    {
                        PGUtil.WriteInt32(_stream, Core.EXTAB_SOCK_DATA);
                        PGUtil.WriteInt32(_stream, dataBytes.Length);
                        WriteSpan(dataBytes);
                        Flush();
                    }
                    _logger?.LogDebug("No. of bytes sent to BE: {BytesSent}", dataBytes.Length);
                }
                ArrayPool<byte>.Shared.Return(bytesBuffer);
                ArrayPool<char>.Shared.Return(charsBuffer);
                PGUtil.WriteInt32(_stream, Core.EXTAB_SOCK_DONE);
                Flush();
                _logger?.LogInformation("sent EXTAB_SOCK_DONE to reader");
            }
        }
        catch (Exception)
        {
            _logger?.LogWarning("Error opening file");
        }
    }
    private bool GetFileFromBE(string logDir, string filename, int logType)
    {
        bool status = true;

        // If no explicit -logDir mentioned (defaulted by backend to /tmp)
        string fullpath = Path.Combine(logDir, filename);

        FileStream? fh = null;
        if (logType == 1)
        {
            fullpath += ".nzlog";
            fh = new FileStream(fullpath, FileMode.Create, FileAccess.Write);
        }
        else if (logType == 2)
        {
            fullpath += ".nzbad";
            fh = new FileStream(fullpath, FileMode.Create, FileAccess.Write);
        }
        else if (logType == 3)
        {
            fullpath += ".nzstats";
            fh = new FileStream(fullpath, FileMode.Create, FileAccess.Write);
        }
        if (fh is null)
        {
            throw new NullReferenceException(nameof(fh));
        }

        using (StreamWriter writer = new StreamWriter(fh, Encoding.UTF8))
        {
            while (true)
            {
                int numBytes = PGUtil.ReadInt32(_stream);

                if (numBytes == 0)  // zeros means EOF, no more data
                {
                    break;
                }
                RegenerateBuffer(numBytes);
                var data = Read(numBytes, _tmp_buffer);
                if (status)
                {
                    try
                    {
                        int maxCharCount = NzConnectionHelpers.ClientEncoding.GetMaxCharCount(numBytes);
                        char[] tmpChars = ArrayPool<char>.Shared.Rent(maxCharCount);
                        int charsWritten = NzConnectionHelpers.ClientEncoding.GetChars(data, 0, numBytes, tmpChars, 0);
                        writer.Write(tmpChars,0, charsWritten);
                        writer.Flush();
                        ArrayPool<char>.Shared.Return(tmpChars);
                        _logger?.LogInformation("Successfully written data into file: {FullPath}", fullpath);
                    }
                    catch (Exception)
                    {
                        _logger?.LogWarning("Error in writing data to file");
                        status = false;
                    }
                }
            }
        }

        return status;
    }

    private void ReceiveAndWriteDataToExternal(FileStream fh)
    {
        PGUtil.Skip4Bytes(_stream);

        while (true)
        {
            // Get EXTAB_SOCK Status
            int status;
            try
            {
                status = PGUtil.ReadInt32(_stream);
            }
            catch (Exception)
            {
                _logger?.LogWarning("Error while retrieving status, closing unload file");
                fh.Close();
                return;
            }

            if (status == Core.EXTAB_SOCK_DATA)
            {
                // get number of bytes in block
                int numBytes = PGUtil.ReadInt32(_stream);
                try
                {
                    byte[] bytes = ArrayPool<byte>.Shared.Rent(numBytes);
                    bytes = Read(numBytes, bytes);
                    fh.Write(bytes, 0, numBytes);
                    fh.Flush();
                    ArrayPool<byte>.Shared.Return(bytes);
                    _logger?.LogInformation("Successfully written data into file");
                }
                catch (Exception)
                {
                    _logger?.LogWarning("Error in writing data to file");
                }
                continue;
            }

            if (status == Core.EXTAB_SOCK_DONE)
            {
                fh.Close();
                _logger?.LogInformation("unload - done receiving data");
                break;
            }

            if (status == Core.EXTAB_SOCK_ERROR)
            {
                //int len = HUnpack(_read(2));
                short len = PGUtil.ReadInt16(_stream);

                string errorMsg = NzConnectionHelpers.ClientEncoding.GetString(Read(len));
                //len = HUnpack(_read(2));
                len = PGUtil.ReadInt16(_stream);
                string errorObject = NzConnectionHelpers.ClientEncoding.GetString(Read(len));

                _logger?.LogWarning("unload - ErrorMsg: {ErrorMsg}", errorMsg);
                _logger?.LogWarning("unload - ErrorObj: {ErrorObject}", errorObject);

                fh.Close();
                _logger?.LogDebug("unload - done receiving data");
                return;
            }
            else
            {
                fh.Close();
                return;
            }
        }
    }

    private void ResGetDbosColumnDescriptions(byte[] data)
    {
        int dataIdx = 0;
        _tupdesc.Version = IUnpack(data, dataIdx);
        _tupdesc.NullsAllowed = IUnpack(data, dataIdx + 4);
        _tupdesc.SizeWord = IUnpack(data, dataIdx + 8);
        _tupdesc.SizeWordSize = IUnpack(data, dataIdx + 12);
        _tupdesc.NumFixedFields = IUnpack(data, dataIdx + 16);
        _tupdesc.NumVaryingFields = IUnpack(data, dataIdx + 20);
        _tupdesc.FixedFieldsSize = IUnpack(data, dataIdx + 24);
        _tupdesc.MaxRecordSize = IUnpack(data, dataIdx + 28);
        _tupdesc.NumFields = IUnpack(data, dataIdx + 32);

        dataIdx += 36;
        for (int ix = 0; ix < _tupdesc.NumFields; ix++)
        {
            //https://github.com/IBM/nzpy/issues/61
            var ft = IUnpack(data, dataIdx);

            if (ft == NzTypeInt && _nzCommand?.NewPreparedStatement?.Description?[ix].TypeOID == 702)
            {
                _tupdesc.FieldType.Add(NzTypeIntvsAbsTimeFIX);
            }
            else
            {
                _tupdesc.FieldType.Add(ft);
            }

            _tupdesc.FieldSize.Add(IUnpack(data, dataIdx + 4));
            _tupdesc.FieldTrueSize.Add(IUnpack(data, dataIdx + 8));
            _tupdesc.FieldOffset.Add(IUnpack(data, dataIdx + 12));
            _tupdesc.FieldPhysField.Add(IUnpack(data, dataIdx + 16));
            _tupdesc.FieldLogField.Add(IUnpack(data, dataIdx + 20));
            _tupdesc.FieldNullAllowed.Add(IUnpack(data, dataIdx + 24) != 0);
            _tupdesc.FieldFixedSize.Add(IUnpack(data, dataIdx + 28));
            _tupdesc.FieldSpringField.Add(IUnpack(data, dataIdx + 32));
            dataIdx += 36;
        }

        _tupdesc.DateStyle = IUnpack(data, dataIdx);
        _tupdesc.EuroDates = IUnpack(data, dataIdx + 4);
    }


    private byte[] _tmp_buffer;
    private RowValue[]? _row;


    public bool UseStringPool { get; set; } = true;

    private string GetStandardString(int curField, Span<byte> spanData, Encoding encoding)
    {
        var sp = _nzCommand?.GetColumnStringPool(curField);
        if (UseStringPool && sp is not null)
        {
            return sp.GetString(spanData, encoding);
        }
        else
        {
            return encoding.GetString(spanData);
        }
    }
    
    private string GetFixedLenString(int curField, Span<byte> fieldDataP, int fldlen, int cursize)
    {
        Span<char> chars = fldlen < 120 ? stackalloc char[fldlen] : new char[fldlen];
        NzConnectionHelpers.ClientEncoding.TryGetChars(fieldDataP.Slice(2, cursize), chars, out int charsRead);
        chars[charsRead..fldlen].Fill(' ');
        var spanData = chars[0..fldlen];
        var sp = _nzCommand?.GetColumnStringPool(curField);
        if (UseStringPool && sp is not null)
        {
            return sp.GetString(spanData);
        }
        else
        {
            return new string(spanData);
        }
    }

    /// <summary>
    /// reading rows = standard. most common way
    /// main hot path
    /// </summary>
    /// <param name="nzCommand"></param>
    /// <param name="tupdesc">can by used local _tupdesc ?? </param>
    /// boxing TODO, (row[fieldLf] = value, where row is object[] and value is int/long etc..)
    /// use string interning ?
    /// use some "field value struct instead of object? " - to reduce boxing (like FieldInfo in SpreadSheetTasks)
    private void ResReadDbosTuple(NzCommand nzCommand)
    {
        int numFields = _tupdesc.NumFields;
        int length = PGUtil.ReadInt32(_stream);//row length

        _logger?.LogDebug("Length of the message from backend: {Length}", length);
        length = PGUtil.ReadInt32(_stream);//we must skip 4 bytes ?
        _logger?.LogDebug("Length of the message from backend: {Length}", length);

        RegenerateBuffer(length);
        byte[] data = Read(length, _tmp_buffer);
        _logger?.LogDebug("Actual message is: {Data}", BitConverter.ToString(data,0,length));

        if (_row is null || _row.Length < numFields)
        {
            _row = new RowValue[numFields];
        }

        int fieldLf = 0;
        int curField = 0;

        while (fieldLf < numFields && curField < numFields)
        {
            ref RowValue rowValue = ref _row[fieldLf];
            //CTableFieldAt can span be used here ? - to reduce alocation
            Span<byte> fieldDataP = CTableFieldAt(data, curField);

            //var standardImplementation = bitmap[tupdesc.FieldPhysField[fieldLf]] == 1;
            //Debug.Assert(standardImplementation == res);

            // a bitmap with value of 1 denotes null column
            if (ColumnIsNull(_tupdesc, data, fieldLf))
            {
                rowValue.typeCode = TypeCodeEx.Empty;
                _logger?.LogDebug("field={Field}, value= NULL", curField + 1);
                curField += 1;
                fieldLf += 1;
                continue;
            }

            // Fldlen is byte-length of backend-datatype
            // memsize is byte-length of ODBC-datatype or internal-datatype for (Numeric/Interval)
            int fldlen = CTableIFieldSize(curField);
            int fldtype = CTableIFieldType(curField);

            if (fldtype == NzTypeUnknown)
            {
                fldtype = NzTypeVarChar;
            }            

            if (fldtype == NzTypeChar)
            {
                string value = GetStandardString(curField, fieldDataP.Slice(0, fldlen), NzConnectionHelpers.CharVarcharEncoding);
                rowValue.typeCode = TypeCodeEx.String;
                rowValue.stringValue = value;
                _logger?.LogDebug("field={Field}, datatype=CHAR, value={Value}", curField + 1, value);
            }

            if (fldtype == NzTypeNChar || fldtype == NzTypeNVarChar)
            {
                int cursize = BitConverter.ToInt16(fieldDataP) - 2;
                string value;
                if (fldtype == NzTypeNVarChar || fldlen == cursize)
                {
                    value = GetStandardString(curField, fieldDataP.Slice(2, cursize), NzConnectionHelpers.ClientEncoding);
                }
                else
                {
                    value = GetFixedLenString(curField, fieldDataP, fldlen, cursize);
                }
                rowValue.typeCode = TypeCodeEx.String;
                rowValue.stringValue = value;
                _logger?.LogDebug("field={Field}, datatype={Datatype}, value={Value}", curField + 1, fldtype.ToString(), value);
            }

            if (fldtype == NzTypeVarChar || fldtype == NzTypeVarFixedChar || fldtype == NzTypeGeometry ||
                fldtype == NzTypeVarBinary || fldtype == NzTypeJson || fldtype == NzTypeJsonb || fldtype == NzTypeJsonpath)
            {
                int cursize = BitConverter.ToInt16(fieldDataP) - 2;
                string value = GetStandardString(curField, fieldDataP.Slice(2, cursize), NzConnectionHelpers.CharVarcharEncoding);
                rowValue.typeCode = TypeCodeEx.String;
                rowValue.stringValue = value;
                _logger?.LogDebug("field={Field}, datatype={Datatype}, value={Value}", curField + 1, fldtype.ToString(), value);
            }

            if (fldtype == NzTypeInt8)  // int64
            {
                long value = BitConverter.ToInt64(fieldDataP);
                rowValue.typeCode = TypeCodeEx.Int64;
                rowValue.int64Value = value;
                _logger?.LogDebug("field={Field}, datatype=NzTypeInt8, value={Value}", curField + 1, value);
            }
            else if (fldtype == NzTypeIntvsAbsTimeFIX) //https://github.com/IBM/nzpy/issues/61 //TODO, SELECT CREATEDATE FROM SYSTEM.ADMIN._V_TABLE_STORAGE_STAT
            {
                DateTime value = DateTypes.TimestampRecvInt(fieldDataP);
                rowValue.typeCode = TypeCodeEx.DateTime;
                rowValue.dateTimeValue = value;
                _logger?.LogDebug("field={Field}, datatype=NzTypeInt4, value={Value}", curField + 1, value);
            }
            else if (fldtype == NzTypeInt)  // int32
            {
                int value = BitConverter.ToInt32(fieldDataP);
                rowValue.typeCode = TypeCodeEx.Int32;
                rowValue.int32Value = value;
                _logger?.LogDebug("field={Field}, datatype=NzTypeInt4, value={Value}", curField + 1, value);
            }
            else if (fldtype == NzTypeInt2)  // int16
            {
                short value = BitConverter.ToInt16(fieldDataP);
                rowValue.typeCode = TypeCodeEx.Int16;
                rowValue.int16Value = value;
                _logger?.LogDebug("field={Field}, datatype=NzTypeInt2, value={Value}", curField + 1, value);
            }
            else if (fldtype == NzTypeInt1)
            {
                //sbyte value = (sbyte)fieldDataP[0];
                Int16 value = (Int16)(sbyte)fieldDataP[0]; // int 16 to be in pair with ODBC
                rowValue.typeCode = TypeCodeEx.Int16; //fix to byte ? 
                rowValue.int16Value = value;
                _logger?.LogDebug("field={Field}, datatype=NzTypeInt1, value={Value}", curField + 1, value);
            }
            else if (fldtype == NzTypeDouble)
            {
                double value = BitConverter.ToDouble(fieldDataP);
                rowValue.typeCode = TypeCodeEx.Double;
                rowValue.doubleValue = value;
                _logger?.LogDebug("field={Field}, datatype=NzTypeDouble, value={Value}", curField + 1, value);
            }
            else if (fldtype == NzTypeFloat)
            {
                float value = BitConverter.ToSingle(fieldDataP);
                rowValue.typeCode = TypeCodeEx.Single;
                rowValue.singleValue = value;
                _logger?.LogDebug("field={Field}, datatype=NzTypeFloat, value={Value}", curField + 1, value);
            }
            else if (fldtype == NzTypeDate)
            {
                DateTime value = DateTypes.ToDateTimeFrom4Bytes(fieldDataP);
                rowValue.typeCode = TypeCodeEx.DateTime;
                rowValue.dateTimeValue = value;
                _logger?.LogDebug("field={Field}, datatype=DATE, value={Value}", curField + 1, value);
            }
            else if (fldtype == NzTypeTime)
            {
                TimeSpan value = DateTypes.TimeRecvFloatX2(fieldDataP);
                rowValue.typeCode = TypeCodeEx.TimeSpan;
                rowValue.timeSpanValue = value;
                _logger?.LogDebug("field={Field}, datatype=TIME, value={Value}", curField + 1, value);
            }
            else if (fldtype == NzTypeInterval)
            {
                string value = DateTypes.TimeRecvFloatX1(fieldDataP);
                rowValue.typeCode = TypeCodeEx.String;
                rowValue.stringValue = value;
                _logger?.LogDebug("field={Field}, datatype=INTERVAL, value={Value}", curField + 1, value);
            }
            else if (fldtype == NzTypeTimeTz) // https://www.ibm.com/docs/en/netezza?topic=tdt-time-time-zone-timetz
            {
                TimeSpan timeSpanVal = DateTypes.TimeRecvFloatX2(fieldDataP);
                int timetzZone = BitConverter.ToInt32(fieldDataP.Slice(fldlen - 4));
                rowValue.typeCode = TypeCodeEx.String;
                rowValue.stringValue = DateTypes.TimetzOutTimetzadt(timeSpanVal, timetzZone);
                //rowValue.stringValue = value.ToString();
                _logger?.LogDebug("field={Field}, datatype=TIMETZ, value={Value}", curField + 1, rowValue.stringValue);
            }
            else if (fldtype == NzTypeTimestamp)
            {
                DateTime value = DateTypes.ToDateTimeFrom8Bytes(fieldDataP);
                rowValue.typeCode = TypeCodeEx.DateTime;
                rowValue.dateTimeValue = value;
                _logger?.LogDebug("field={Field}, datatype=TIMESTAMP, value={Value}", curField + 1, value);
            }
            else if (fldtype == NzTypeNumeric)
            {
                int prec = CTableIFieldPrecision(curField);
                int scale = CTableIFieldScale(curField);
                int count = CTableIFieldNumericDigit32Count(curField);
                decimal? value = Numeric.GetCsNumeric(fieldDataP, prec, scale, count);
                rowValue.typeCode = TypeCodeEx.Decimal;
                rowValue.decimalValue = value ?? 9999.99m;
                _logger?.LogDebug("field={Field}, datatype=NUMERIC, value={Value}", curField + 1, value);
            }
            else if (fldtype == NzTypeBool)
            {
                bool value = fieldDataP[0] == 0x01;
                rowValue.typeCode = TypeCodeEx.Boolean;
                rowValue.boolValue = value;
                _logger?.LogDebug("field={Field}, datatype=BOOL, value={Value}", curField + 1, value);
            }

            curField += 1;
            fieldLf += 1;
        }

        nzCommand.AddRow(_row);

    }


    private void RegenerateBuffer(int length)
    {
        if (_tmp_buffer.Length < length)
        {
            ArrayPool<byte>.Shared.Return(_tmp_buffer);
            _tmp_buffer = ArrayPool<byte>.Shared.Rent(length);
        }
    }

    private static bool ColumnIsNull(DbosTupleDesc tupdesc, byte[] data, int fieldLf)
    {
        var decodedColumnNumber = tupdesc.FieldPhysField[fieldLf];
        byte numberToTest = data[2 + decodedColumnNumber / 8];
        var numberOfBitToCheck = decodedColumnNumber % 8;
        var columnIsNull = (numberToTest & (1 << numberOfBitToCheck)) != 0;
        return tupdesc.NullsAllowed != 0 && columnIsNull;
    }

    private int CTableIFieldPrecision(int coldex)
    {
        return ((_tupdesc.FieldSize[coldex] >> 8) & 0x7F);
    }

    internal int CTableIFieldScale(int coldex)
    {
        return (_tupdesc.FieldSize[coldex] & 0x00FF);
    }
    internal int CTableIFieldScaleAlternative(int coldex)
    {
        var typeModyfier = _nzCommand.NewPreparedStatement!.Description![coldex].TypeModifier;
        typeModyfier &= 0b111111;
        return ((typeModyfier >> 3) - 2) * 8 + (typeModyfier & 0b000111);
    }

    //public string TypeModifierBinary(int coldex)
    //{
    //    var typeModyfier = _nzCommand.NewPreparedStatement!.Description![coldex].TypeModifier;
    //    typeModyfier &= 0b111111;
    //    var xx  =  ((typeModyfier >> 3) - 2) * 8 + (typeModyfier & 0b000111);
    //    return xx.ToString() + "_" + Convert.ToString(_nzCommand.NewPreparedStatement!.Description![coldex].TypeModifier ,2).PadLeft(32,'0');
    //}


    private int CTableIFieldNumericDigit32Count(int coldex)
    {
        int sizeTNumericDigit = 4;
        return _tupdesc.FieldTrueSize[coldex] / sizeTNumericDigit;
    }
    internal bool IsExtendedRowDescriptionAvaiable() => _tupdesc is not null;


    private int CTableIFieldType(int curField)
    {
        return _tupdesc.FieldType[curField];
    }

    private int CTableIFieldSize(int curField)
    {
        return _tupdesc.FieldSize[curField];
    }

    private Span<byte> CTableFieldAt(byte[] data, int curField)
    {
        if (_tupdesc.FieldFixedSize[curField] != 0)
        {
            return CTableIFixedFieldPtr(data, _tupdesc.FieldOffset[curField]);
        }

        return NzConnection.CTableIVarFieldPtr(data, _tupdesc.FixedFieldsSize, _tupdesc.FieldOffset[curField]);
    }

    private static Span<byte> CTableIVarFieldPtr(byte[] data, int fixedOffset, int varDex)
    {
        Span<byte> lenP = data.AsSpan().Slice(fixedOffset);
        for (int ctr = 0; ctr < varDex; ctr++)
        {
            int length = BitConverter.ToInt16(lenP);
            if (length % 2 == 0)
            {
                lenP = lenP.Slice(length);
            }
            else
            {
                lenP = lenP.Slice(length + 1);
            }
        }

        return lenP;
    }

    private static Span<byte> CTableIFixedFieldPtr(byte[] data, int offset)
    {
        return data.AsSpan()[offset..];
    }

    //only for system tabeles  + selects without from ? -> "SELECT * FROM _V_TABLE"  or "SELECT 123"
    private void HandleDataRow(byte[] data, NzCommand nzCommand)
    {
        // bitmaplen denotes the number of bytes bitmap sent by backend.
        // For e.g.: for select statement with 9 columns, we would receive 2 bytes bitmap.
        int numberOfCol = nzCommand.NewPreparedStatement!.FieldCount;
        int bitmapLen = numberOfCol / 8;
        if ((numberOfCol % 8) > 0)
        {
            bitmapLen += 1;
        }

        int dataIdx = bitmapLen;
        if (_row is null || _row.Length < numberOfCol)
        {
            _row = new RowValue[numberOfCol];
        }

        for (int columnNumber = 0; columnNumber < numberOfCol; columnNumber++)
        {
            var byteToTest = (byte)data[columnNumber / 8];
            var positionInByteToTest = 7 - columnNumber % 8;
            var nullHelpValue = byteToTest & (1 << positionInByteToTest);
            ref RowValue rowValue = ref _row[columnNumber];
            if (nullHelpValue == 0)
            {
                rowValue.typeCode = TypeCodeEx.Empty;
            }
            else
            {
                var typeOid = nzCommand.NewPreparedStatement.Description![columnNumber].TypeOID;
                Sylvan? sp = UseStringPool ? nzCommand.GetColumnStringPool(columnNumber) : null;
                int vlen = IUnpack(data, dataIdx);
                dataIdx += 4;

                switch (typeOid)
                {
                    case 16: // boolean
                        rowValue.typeCode = TypeCodeEx.Boolean;
                        rowValue.boolValue = NzConnectionHelpers.BoolRecvTyped(data, dataIdx, vlen - 4);
                        break;
                    case 17: // bytea
                        rowValue.typeCode = TypeCodeEx.String; 
                        rowValue.stringValue = NzConnectionHelpers.ByteaRecv(data, dataIdx, vlen - 4);
                        break;
                    case 19: // name type
                        rowValue.typeCode = TypeCodeEx.String;
                        rowValue.stringValue = NzConnectionHelpers.TextRecv(data, dataIdx, vlen - 4, sp);
                        break;
                    case 20: // int8
                        rowValue.typeCode = TypeCodeEx.Int64;
                        rowValue.int64Value = NzConnectionHelpers.Int8RecvTyped(data, dataIdx, vlen - 4);
                        break;
                    case 21: // int2
                        rowValue.typeCode = TypeCodeEx.Int16;
                        rowValue.int16Value = NzConnectionHelpers.Int2RecvTyped(data, dataIdx, vlen - 4);
                        break;
                    case 23: // int4
                        rowValue.typeCode = TypeCodeEx.Int32;
                        rowValue.int32Value = NzConnectionHelpers.Int4RecvTyped(data, dataIdx, vlen - 4);
                        break;
                    case 25: // TEXT type
                        rowValue.typeCode = TypeCodeEx.String;
                        rowValue.stringValue = NzConnectionHelpers.TextRecv(data, dataIdx, vlen - 4, sp);
                        break;
                    case 26: // oid
                        rowValue.typeCode = TypeCodeEx.Int32;
                        rowValue.int32Value = NzConnectionHelpers.Int4RecvTyped(data, dataIdx, vlen - 4);
                        break;
                    case 28: // xid
                        rowValue.typeCode = TypeCodeEx.Int32;
                        rowValue.int32Value = NzConnectionHelpers.Int4RecvTyped(data, dataIdx, vlen - 4);
                        break;
                    case 700: // float4
                        rowValue.typeCode = TypeCodeEx.Single;
                        rowValue.singleValue = NzConnectionHelpers.Float4RecvTyped(data, dataIdx, vlen - 4);
                        break;
                    case 701: // float8
                        rowValue.typeCode = TypeCodeEx.Double;
                        rowValue.doubleValue = NzConnectionHelpers.Float8RecvTyped(data, dataIdx, vlen - 4);
                        break;
                    case 702: // SELECT CREATEDATE FROM _V_TABLE ORDER BY CREATEDATE DESC .. 
                        rowValue.typeCode = TypeCodeEx.DateTime; //with time
                        rowValue.dateTimeValue = DateTypes.TimestamptzRecvFloatTyped(data, dataIdx, vlen - 4);
                        break;
                    case 705: // unknown
                        rowValue.typeCode = TypeCodeEx.String;
                        rowValue.stringValue = NzConnectionHelpers.TextRecv(data, dataIdx, vlen - 4, sp);
                        break;
                    case 829: // MACADDR type
                        rowValue.typeCode = TypeCodeEx.String;
                        rowValue.stringValue = NzConnectionHelpers.TextRecv(data, dataIdx, vlen - 4, sp);
                        break;
                    case 1042: // CHAR type
                        rowValue.typeCode = TypeCodeEx.String;
                        rowValue.stringValue = NzConnectionHelpers.TextRecv(data, dataIdx, vlen - 4, sp);
                        break;
                    case 1043: // VARCHAR type
                        rowValue.typeCode = TypeCodeEx.String;
                        rowValue.stringValue = NzConnectionHelpers.TextRecv(data, dataIdx, vlen - 4, sp);
                        break;
                    case 1082: // date
                        rowValue.typeCode = TypeCodeEx.DateTime;//without time
                        rowValue.dateTimeValue = DateTypes.DateInTyped(data, dataIdx, vlen - 4);
                        break;
                    case 1083: // time
                        rowValue.typeCode = TypeCodeEx.TimeSpan;
                        rowValue.timeSpanValue = DateTypes.TimeInTyped(data, dataIdx, vlen - 4);
                        break;
                    case 1114: // timestamp w/ tz
                        rowValue.typeCode = TypeCodeEx.DateTime;
                        rowValue.dateTimeValue = DateTypes.TimestampRecvFloatTyped(data, dataIdx, vlen - 4);
                        break;
                    case 1184:
                        rowValue.typeCode = TypeCodeEx.DateTime;
                        rowValue.dateTimeValue = DateTypes.TimestamptzRecvFloatTyped(data, dataIdx, vlen - 4);
                        break;
                    case 1186:
                        rowValue.typeCode = TypeCodeEx.String;
                        rowValue.stringValue = NzConnectionHelpers.IntervalRecvInteger(data, dataIdx, vlen - 4);
                        break;
                    case 1700: // NUMERIC
                        rowValue.typeCode = TypeCodeEx.Decimal;
                        rowValue.decimalValue = NzConnectionHelpers.NumericInTyped(data, dataIdx, vlen - 4);
                        break;
                    case 2275: // cstring
                        rowValue.typeCode = TypeCodeEx.String;
                        rowValue.stringValue = NzConnectionHelpers.TextRecv(data, dataIdx, vlen - 4, sp);
                        break;
                    case 2500: // SELECT 15::BYTEINT
                        rowValue.typeCode = TypeCodeEx.Int16;
                        rowValue.int16Value = NzConnectionHelpers.ByteRecvTyped(data, dataIdx, vlen - 4);
                        break;
                    case 2950: // uuid
                        rowValue.typeCode = TypeCodeEx.String;
                        rowValue.stringValue = NzConnectionHelpers.UuidRecvTyped(data, dataIdx, vlen - 4).ToString() ?? "no uuid";
                        break;
                    default:
                        rowValue.typeCode = TypeCodeEx.String;
                        rowValue.stringValue = NzConnectionHelpers.TextRecv(data, dataIdx, vlen - 4, sp);
                        break;
                }
                //TODO { 22, (FC_TEXT, VectorIn) },       // int2vector
                //TODO{ 114, (FC_TEXT, JsonIn) },        // json
                //TODO{ 1000, (FC_BINARY, ArrayRecv) },  // BOOL[]
                //TODO{ 1003, (FC_BINARY, ArrayRecv) },  // NAME[]
                //TODO{ 1005, (FC_BINARY, ArrayRecv) },  // INT2[]
                //TODO{ 1007, (FC_BINARY, ArrayRecv) },  // INT4[]
                //TODO{ 1009, (FC_BINARY, ArrayRecv) },  // TEXT[]
                //TODO{ 1014, (FC_BINARY, ArrayRecv) },  // CHAR[]
                //TODO{ 1015, (FC_BINARY, ArrayRecv) },  // VARCHAR[]
                //TODO{ 1016, (FC_BINARY, ArrayRecv) },  // INT8[]
                //TODO{ 1021, (FC_BINARY, ArrayRecv) },  // FLOAT4[]
                //TODO{ 1022, (FC_BINARY, ArrayRecv) },  // FLOAT8[]
                //TODO{ 1231, (FC_TEXT, ArrayIn) },      // NUMERIC[]
                //TODO{ 1263, (FC_BINARY, ArrayRecv) },  // cstring[]
                //TODO{{ 3802, (FC_TEXT, JsonIn) }        // jsonb
                dataIdx += vlen - 4;
            }
        }
        nzCommand.AddRow(_row);
    }

    private void HandleCommandComplete(byte[] data, int length, NzCommand nzCommand)
    {
        var values = Encoding.UTF8.GetString(data, 0, length - 1).Split(' ');
        var command = values[0];
        if (_commandsWithCount.Contains(command))
        {
            int rowCount = int.Parse(values[^1]);
            if (nzCommand._recordsAffected == -1)
            {
                nzCommand._recordsAffected = rowCount;
            }
            else
            {
                nzCommand._recordsAffected += rowCount;
            }
        }

        //if (command == "ALTER" || command == "CREATE")
        //{
        //    foreach (var scache in _caches.Values)
        //    {
        //        foreach (var pcache in scache.Values)
        //        {
        //            foreach (var ps in pcache["ps"].Values)
        //            {
        //                ClosePreparedStatement(ps["statement_name_bin"] as byte[]);

        //            }
        //            pcache["ps"].Clear();
        //        }
        //    }
        //}
    }
    private static void HandleRowDescription(byte[] data, NzCommand nzCommand)
    {
        int count = HUnpack(data);
        int idx = 2;

         nzCommand.NewPreparedStatement!.Description = new RowDescriptionMessage(count);

        for (int i = 0; i < count; i++)
        {
            int nullByteIndex = Array.IndexOf(data, (byte)0x00, idx);
            byte[] nameBytes = data[idx..nullByteIndex];
            string name = Encoding.UTF8.GetString(nameBytes);
            idx += nameBytes.Length + 1;

            var (typeOid, typeSize, typeModifier, format) = IHICUnpack(data, idx);
            //var receiver = NzConnectionHelpers.GetPgTypeX(typeOid);

            var fieldNew = new FieldDescription
            {
                Name = name,
                TypeOID = (uint)typeOid,
                TypeSize = typeSize,
                TypeModifier = typeModifier,
                DataFormat = format,
                //CalculationFunc = receiver
            };
            if (fieldNew.Type == typeof(string))
            {
                fieldNew.StringPool = new Sylvan();
            }

            nzCommand.NewPreparedStatement.Description[i] = fieldNew;
            idx += 11;
        }
    }

    //I = int = 32
    //H = short = 16
    //C = byte

    //PGUtil, ReadInt32
    private static int IUnpack(byte[] data, int offset = 0)
    {
        if (BitConverter.IsLittleEndian)
        {
            return (data[offset] << 24) | (data[offset + 1] << 16) |
                   (data[offset + 2] << 8) | data[offset + 3];
            // or BitConverter.ToInt32(data, offset) + 

        }
        else
        {
            return BitConverter.ToInt32(data, offset);
        }
    }

    private static short HUnpack(byte[] data, int offset = 0)
    {
        // Convert network byte order (big-endian) to host byte order
        if (BitConverter.IsLittleEndian)
        {
            return (short)((data[offset] << 8) | data[offset + 1]);
        }
        else
        {
            return BitConverter.ToInt16(data, offset);
        }
    }

    //private (byte messageCode, int dataLen) CiUnpack(byte[] data)
    //{
    //    if (data == null || data.Length < 5)
    //        throw new ArgumentException("Invalid data length for unpacking");

    //    return (
    //        messageCode: data[0],
    //        dataLen: IUnpack(data, 1)
    //    );
    //}

    public static (int, short, int, byte) IHICUnpack(byte[] data, int index)
    {
        if (data.Length < 11)
        {
            throw new ArgumentException("Data array is too short.");
        }

        int i1 = IUnpack(data, index + 0);
        short s = HUnpack(data, index + 4);
        int i3 = IUnpack(data, index + 6);
        byte b = data[10];

        return (i1, s, i3, b);
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        throw new NotImplementedException();
    }

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotImplementedException();
    }

    public override void Close()
    {
        _stream?.Dispose();
        _stream = null!;

        _socket?.Dispose();
        _socket = null!;
        _nzCommand = null!;
        _state = ConnectionState.Closed;
        ArrayPool<byte>.Shared.Return(_tmp_buffer);
    }

    private NzCommand _nzCommand = null!;

    protected override DbCommand CreateDbCommand()
    {
        _nzCommand = new NzCommand(this);
        return _nzCommand;
    }

    public override void Open()
    {
        Open();
    }
    public void Open(ClientTypeId clientVersionId = ClientTypeId.SqlDotnet)
    {
        _state = ConnectionState.Connecting;
        _stream = Initialize(_host, _port);
        Handshake handShake = new(_socket, _stream, _host, _sslCerFilePath, _logger)
        {
            NPSCLIENT_TYPE_PYTHON = clientVersionId
        };
        Stream? response = handShake.Startup(_database, _securityLevel, _user, _password, _pgOptions);
        _backendKeyData = handShake.BackendKeyData;

        if (response is not null)
        {
            _stream = response;
        }
        else
        {
            throw new NetezzaException("Error in handshake");
        }

        _nzCommand = (NzCommand)CreateCommand();
        if (!ConnSendQuery())
        {
            _logger?.LogWarning("Error sending initial setup queries");
        }
        _commandNumber = 0;

        InTransaction = false;
        _state = ConnectionState.Open;
    }
}

public sealed class NetezzaException : DbException
{
    public NetezzaException() : base() { }
    public NetezzaException(string msg) : base(msg) { }
    public NetezzaException(string msg, Exception exception) : base(msg, exception) { }
    public NetezzaException(Exception exception) : base("", exception) { }
}
public sealed class InterfaceException : DbException
{
    public InterfaceException(): base() { }
    public InterfaceException( string msg) : base(msg) { }
    public InterfaceException(string msg, Exception exception) : base(msg, exception) { }
}
public sealed class AttributeException : DbException { }
//public class NotSupportedException : DbException { }
//public class ConnectionClosedException : DbException { }
//public class DatabaseException : DbException { }
//public class OperationalException : DbException { }
//public class IntegrityException : DbException { }
//public class InternalException : DbException { }
