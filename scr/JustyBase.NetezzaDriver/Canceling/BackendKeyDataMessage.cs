namespace JustyBase.NetezzaDriver;

/// <summary>
/// Represents a message containing backend key data from the server.
/// </summary>
internal sealed class BackendKeyDataMessage
{
    /// <summary>
    /// Gets the backend process ID.
    /// </summary>
    internal int BackendProcessId { get; }

    /// <summary>
    /// Gets the backend secret key.
    /// </summary>
    internal int BackendSecretKey { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="BackendKeyDataMessage"/> class.
    /// </summary>
    /// <param name="stream">The stream to read the backend key data from.</param>
    public BackendKeyDataMessage(Stream stream)
    {
        BackendProcessId = PGUtil.ReadInt32(stream);
        BackendSecretKey = PGUtil.ReadInt32(stream);
    }
}
