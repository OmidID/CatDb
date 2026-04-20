namespace CatDb.General.Communication;

/// <summary>
/// A single request/response pair in the CatDb wire protocol.
/// Wire frame: Int64 id (LE) | Int32 size (LE) | byte[size] payload
///
/// On the <b>client side</b> the caller awaits <see cref="WaitAsync"/> after
/// enqueuing the packet – no thread ever blocks on I/O.
///
/// On the <b>server side</b> the same object carries the inbound request; the
/// handler writes to <see cref="Response"/> and enqueues it for sending.
/// </summary>
public sealed class Packet
{
    private readonly TaskCompletionSource<MemoryStream> _tcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal long Id;

    /// <summary>Inbound request payload (set by receive loop or by caller).</summary>
    public MemoryStream Request { get; }

    /// <summary>Outbound response payload – set by the server handler before enqueuing.</summary>
    public MemoryStream? Response { get; set; }

    public Packet(MemoryStream request)
    {
        Request = request ?? throw new ArgumentNullException(nameof(request));
    }

    // ── Client-side API ──────────────────────────────────────────────────────

    /// <summary>
    /// Returns a Task that completes when the server's response arrives.
    /// Pass a <see cref="CancellationToken"/> to support cancellation.
    /// </summary>
    public Task<MemoryStream> WaitAsync(CancellationToken ct = default) =>
        ct == default ? _tcs.Task : _tcs.Task.WaitAsync(ct);

    // ── Called by the receive loop ────────────────────────────────────────────

    internal void SetResponse(MemoryStream ms)  => _tcs.TrySetResult(ms);
    internal void SetException(Exception ex)    => _tcs.TrySetException(ex);
    internal void Cancel()                      => _tcs.TrySetCanceled();
}
