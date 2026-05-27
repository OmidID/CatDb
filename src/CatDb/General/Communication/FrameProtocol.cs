using System.Buffers.Binary;
using System.Net.Sockets;

namespace CatDb.General.Communication;

/// <summary>
/// Low-level wire-frame helpers.
/// Each frame is:  Int64 id (LE) | Int32 size (LE) | byte[size] payload
/// </summary>
internal static class FrameProtocol
{
    private const int HeaderSize = 12; // 8 bytes id + 4 bytes size

    public static async Task WriteAsync(
        NetworkStream stream,
        long id,
        MemoryStream payload,
        CancellationToken ct)
    {
        var size   = (int)payload.Length;
        var header = new byte[HeaderSize];

        BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(0), id);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(8), size);

        await stream.WriteAsync(header.AsMemory(), ct).ConfigureAwait(false);
        await stream.WriteAsync(payload.GetBuffer().AsMemory(0, size), ct).ConfigureAwait(false);
        await stream.FlushAsync(ct).ConfigureAwait(false);
    }

    public static async Task<(long Id, MemoryStream Data)> ReadAsync(
        NetworkStream stream,
        CancellationToken ct)
    {
        var header = new byte[HeaderSize];
        await stream.ReadExactlyAsync(header.AsMemory(), ct).ConfigureAwait(false);

        var id   = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(0));
        var size = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(8));

        var body = new byte[size];
        await stream.ReadExactlyAsync(body.AsMemory(), ct).ConfigureAwait(false);

        return (id, new MemoryStream(body));
    }

    // ── Blocking variants (true sync — no Task/await anywhere) ───────────────

    public static void WriteSync(NetworkStream stream, long id, MemoryStream payload)
    {
        var size   = (int)payload.Length;
        var header = new byte[HeaderSize];

        BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(0), id);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(8), size);

        stream.Write(header, 0, HeaderSize);
        stream.Write(payload.GetBuffer(), 0, size);
        stream.Flush();
    }

    public static (long Id, MemoryStream Data) ReadSync(NetworkStream stream)
    {
        var header = new byte[HeaderSize];
        stream.ReadExactly(header, 0, HeaderSize);

        var id   = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(0));
        var size = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(8));

        var body = new byte[size];
        stream.ReadExactly(body, 0, size);

        return (id, new MemoryStream(body));
    }
}
