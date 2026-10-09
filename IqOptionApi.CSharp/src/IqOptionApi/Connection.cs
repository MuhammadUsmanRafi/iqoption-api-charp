using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace IqOptionApi;

/// <summary>Minimal text WebSocket (a fake replaces it in tests).</summary>
internal interface ISocket : IAsyncDisposable
{
    Task ConnectAsync(Uri uri, CancellationToken ct);

    Task SendAsync(string text, CancellationToken ct);

    /// <summary>Next text message, or null once the socket closed.</summary>
    Task<string?> ReceiveAsync(CancellationToken ct);
}

internal sealed class ClientWebSocketAdapter : ISocket
{
    private readonly ClientWebSocket _ws = new();

    public ClientWebSocketAdapter(IqOptionOptions o, string origin)
    {
        _ws.Options.SetRequestHeader("Origin", origin);
        _ws.Options.KeepAliveInterval = TimeSpan.Zero;
        if (o.ProxyHost != null) _ws.Options.Proxy = Connection.BuildProxy(o);
        if (o.IgnoreCertificateErrors) _ws.Options.RemoteCertificateValidationCallback = (_, _, _, _) => true;
    }

    public Task ConnectAsync(Uri uri, CancellationToken ct) => _ws.ConnectAsync(uri, ct);

    public Task SendAsync(string text, CancellationToken ct) =>
        _ws.SendAsync(Encoding.UTF8.GetBytes(text), WebSocketMessageType.Text, true, ct);

    public async Task<string?> ReceiveAsync(CancellationToken ct)
    {
        var buffer = new byte[64 * 1024];
        using var ms = new MemoryStream();
        while (true)
        {
            WebSocketReceiveResult r;
            try
            {
                r = await _ws.ReceiveAsync(buffer, ct).ConfigureAwait(false);
            }
            catch (WebSocketException)
            {
                return null;
            }
            if (r.MessageType == WebSocketMessageType.Close) return null;
            ms.Write(buffer, 0, r.Count);
            if (r.EndOfMessage) return Encoding.UTF8.GetString(ms.GetBuffer(), 0, (int)ms.Length);
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_ws.State == WebSocketState.Open)
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                await _ws.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "", cts.Token).ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
            // closing a broken socket is best effort
        }
        _ws.Dispose();
    }
}

/// <summary>Completes <see cref="Next"/> every time something changes, so waiters can re-check their condition.</summary>
internal sealed class AsyncPulse
{
    private TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task Next => Volatile.Read(ref _tcs).Task;

    public void Pulse() =>
        Interlocked.Exchange(ref _tcs, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult();
}

/// <summary>Waits for the next message with a name (and optionally a request id).</summary>
internal sealed class Waiter : IDisposable
{
    private readonly Connection _owner;

    internal Waiter(Connection owner, string name, string? requestId)
    {
        _owner = owner;
        Name = name;
        RequestId = requestId;
    }

    public string Name { get; }

    public string? RequestId { get; }

    internal TaskCompletionSource<JsonObject> Source { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task<JsonObject> Task => Source.Task;

    public void Dispose() => _owner.RemoveWaiter(this);
}

/// <summary>HTTP login + one WebSocket + reply matching. Knows nothing about IQ Option methods.</summary>
internal sealed class Connection : IDisposable
{
    public const string DefaultUserAgent =
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36";

    private readonly IqOptionOptions _o;
    private readonly Func<ISocket> _socketFactory;
    private readonly HttpClient _http;
    private readonly Action<string> _log;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _gates = new();
    private readonly object _waitLock = new();
    private readonly List<Waiter> _waiters = new();
    private ISocket? _socket;
    private CancellationTokenSource? _loopCts;
    private int _requestCounter;
    private bool _ready;

    internal Connection(IqOptionOptions o, Func<ISocket>? socketFactory, HttpMessageHandler? httpHandler, Action<string> log)
    {
        _o = o;
        _log = log;
        WebSocketUri = new Uri(o.WebSocketUrl ?? $"wss://{o.Host}/echo/websocket");
        _socketFactory = socketFactory ?? (() => new ClientWebSocketAdapter(o, "http://" + o.Host));
        _http = new HttpClient(httpHandler ?? BuildHandler(o), disposeHandler: true);
    }

    public event Action<JsonObject>? MessageReceived;

    public event Action<string>? Closed;

    public Uri WebSocketUri { get; }

    public AsyncPulse Pulse { get; } = new();

    public bool IsOpen { get; private set; }

    /// <summary>True once the connect sequence finished; normal requests wait for it.</summary>
    public bool Ready
    {
        get => _ready;
        set
        {
            _ready = value;
            Pulse.Pulse();
        }
    }

    public Dictionary<string, string> Headers { get; } = new() { ["User-Agent"] = DefaultUserAgent };

    public Dictionary<string, string> Cookies { get; } = new();

    private string AuthBase => _o.AuthUrl.TrimEnd('/');

    internal static IWebProxy BuildProxy(IqOptionOptions o) =>
        new WebProxy(o.ProxyHost!, o.ProxyPort ?? 80)
        {
            Credentials = o.ProxyUser != null ? new NetworkCredential(o.ProxyUser, o.ProxyPassword) : null,
        };

    private static HttpMessageHandler BuildHandler(IqOptionOptions o)
    {
        var h = new SocketsHttpHandler
        {
            UseCookies = false,
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
            UseProxy = o.ProxyHost != null,
        };
        if (o.ProxyHost != null) h.Proxy = BuildProxy(o);
        if (o.IgnoreCertificateErrors)
            h.SslOptions = new SslClientAuthenticationOptions { RemoteCertificateValidationCallback = (_, _, _, _) => true };
        return h;
    }

    // ------------------------------------------------------------------ HTTP

    public async Task<(int Status, string Body, string? Ssid)> LoginAsync(string email, string password, string? token, CancellationToken ct)
    {
        var fields = new List<KeyValuePair<string, string>> { new("identifier", email), new("password", password) };
        if (token != null) fields.Add(new("token", token));
        using var req = new HttpRequestMessage(HttpMethod.Post, AuthBase + "/api/v2/login") { Content = new FormUrlEncodedContent(fields) };
        using var resp = await SendHttpAsync(req, ct).ConfigureAwait(false);
        string body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        string? ssid = ReadCookie(resp, "ssid");
        if (ssid != null) Cookies["ssid"] = ssid;
        return ((int)resp.StatusCode, body, ssid);
    }

    public async Task<string> TwoFactorAsync(string token, string? method, string? code, CancellationToken ct)
    {
        var content = new StringContent(PyJson.Dumps(new Obj { ["method"] = method, ["token"] = token, ["code"] = code }), Encoding.UTF8);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        using var req = new HttpRequestMessage(HttpMethod.Post, AuthBase + "/api/v2/verify/2fa") { Content = content };
        using var resp = await SendHttpAsync(req, ct).ConfigureAwait(false);
        return await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
    }

    public async Task LogoutAsync(CancellationToken ct)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, AuthBase + "/api/v1.0/logout");
        using var resp = await SendHttpAsync(req, ct).ConfigureAwait(false);
    }

    private async Task<HttpResponseMessage> SendHttpAsync(HttpRequestMessage req, CancellationToken ct)
    {
        foreach (var (key, value) in Headers) req.Headers.TryAddWithoutValidation(key, value);
        if (Cookies.Count > 0)
            req.Headers.TryAddWithoutValidation("Cookie", string.Join("; ", Cookies.Select(c => c.Key + "=" + c.Value)));
        return await _http.SendAsync(req, ct).ConfigureAwait(false);
    }

    private static string? ReadCookie(HttpResponseMessage resp, string name)
    {
        if (!resp.Headers.TryGetValues("Set-Cookie", out var values)) return null;
        foreach (string v in values)
        {
            string first = v.Split(';')[0];
            int eq = first.IndexOf('=');
            if (eq > 0 && first[..eq].Trim() == name) return first[(eq + 1)..].Trim();
        }
        return null;
    }

    // ------------------------------------------------------------- WebSocket

    public string NextRequestId() => Interlocked.Increment(ref _requestCounter).ToString(CultureInfo.InvariantCulture);

    public async Task OpenAsync(CancellationToken ct)
    {
        await CloseSocketAsync().ConfigureAwait(false);
        var socket = _socketFactory();
        await socket.ConnectAsync(WebSocketUri, ct).ConfigureAwait(false);
        Interlocked.Exchange(ref _requestCounter, 0);
        var cts = new CancellationTokenSource();
        _socket = socket;
        _loopCts = cts;
        IsOpen = true;
        _ = Task.Run(() => ReceiveLoopAsync(socket, cts.Token), CancellationToken.None);
        Pulse.Pulse();
    }

    public async Task CloseSocketAsync()
    {
        var socket = _socket;
        var cts = _loopCts;
        _socket = null;
        _loopCts = null;
        IsOpen = false;
        _ready = false;
        cts?.Cancel();
        if (socket != null)
        {
            try
            {
                await socket.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log("socket close: " + ex.Message);
            }
        }
        FailWaiters("connection closed");
        Pulse.Pulse();
    }

    private async Task ReceiveLoopAsync(ISocket socket, CancellationToken ct)
    {
        string reason = "connection closed by server";
        try
        {
            while (!ct.IsCancellationRequested)
            {
                string? text = await socket.ReceiveAsync(ct).ConfigureAwait(false);
                if (text == null) break;
                Dispatch(text);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            reason = ex.Message;
        }
        if (ct.IsCancellationRequested || !ReferenceEquals(socket, _socket)) return;
        IsOpen = false;
        _ready = false;
        FailWaiters("connection lost");
        Pulse.Pulse();
        try
        {
            Closed?.Invoke(reason);
        }
        catch (Exception ex)
        {
            _log("Closed handler threw: " + ex.Message);
        }
    }

    internal void Dispatch(string text)
    {
        if (_o.LogFrames) _log("< " + text);
        JsonObject? message;
        try
        {
            message = JsonNode.Parse(text) as JsonObject;
        }
        catch (JsonException ex)
        {
            _log("unreadable message skipped: " + ex.Message);
            return;
        }
        if (message == null) return;
        try
        {
            MessageReceived?.Invoke(message);
        }
        catch (Exception ex)
        {
            _log("message handler threw: " + ex.Message);
        }
        CompleteWaiters(message);
        Pulse.Pulse();
    }

    // --------------------------------------------------------------- waiting

    public Waiter AddWaiter(string name, string? requestId)
    {
        var w = new Waiter(this, name, requestId);
        lock (_waitLock) _waiters.Add(w);
        return w;
    }

    internal void RemoveWaiter(Waiter w)
    {
        lock (_waitLock) _waiters.Remove(w);
    }

    public SemaphoreSlim Gate(string replyName) => _gates.GetOrAdd(replyName, _ => new SemaphoreSlim(1, 1));

    private void CompleteWaiters(JsonObject m)
    {
        string? name = J.Str(m["name"]);
        string rid = J.Str(m["request_id"]) ?? "";
        var hits = new List<Waiter>();
        lock (_waitLock)
        {
            foreach (var w in _waiters)
            {
                if (w.Name == name && (w.RequestId == null || w.RequestId == rid)) hits.Add(w);
            }
            foreach (var w in hits) _waiters.Remove(w);
        }
        foreach (var w in hits) w.Source.TrySetResult(m);
    }

    private void FailWaiters(string reason)
    {
        List<Waiter> all;
        lock (_waitLock)
        {
            all = _waiters.ToList();
            _waiters.Clear();
        }
        foreach (var w in all) w.Source.TrySetException(new IqOptionException(reason));
    }

    public async Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout, CancellationToken ct)
    {
        var deadline = timeout is TimeSpan t ? DateTime.UtcNow + t : DateTime.MaxValue;
        while (true)
        {
            var next = Pulse.Next;
            if (condition()) return;
            if (timeout == null)
            {
                await next.WaitAsync(ct).ConfigureAwait(false);
                continue;
            }
            var left = deadline - DateTime.UtcNow;
            if (left <= TimeSpan.Zero) throw new TimeoutException("timed out waiting for the server");
            try
            {
                await next.WaitAsync(left, ct).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                if (condition()) return;
                throw;
            }
        }
    }

    // --------------------------------------------------------------- sending

    public async Task SendAsync(string outer, object? msg, string requestId, CancellationToken ct, bool requireReady = true)
    {
        string frame = PyJson.Frame(outer, msg, requestId);
        if (requireReady) await WaitUntilReadyAsync(ct).ConfigureAwait(false);
        try
        {
            await SendOnceAsync(frame, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (requireReady && ex is WebSocketException or ObjectDisposedException or IqOptionException)
        {
            // Not sent: wait for the reconnect and send once more (Python re-sends after reconnecting).
            await WaitUntilReadyAsync(ct).ConfigureAwait(false);
            await SendOnceAsync(frame, ct).ConfigureAwait(false);
        }
    }

    private async Task SendOnceAsync(string frame, CancellationToken ct)
    {
        await _sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var socket = _socket ?? throw new IqOptionException("not connected");
            await socket.SendAsync(frame, ct).ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
        if (_o.LogFrames) _log("> " + frame);
    }

    private async Task WaitUntilReadyAsync(CancellationToken ct)
    {
        if (Ready && IsOpen) return;
        try
        {
            await WaitUntilAsync(() => Ready && IsOpen, _o.RequestTimeout, ct).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            throw new IqOptionException("not connected");
        }
    }

    public async Task<JsonObject> RequestAsync(string outer, object? msg, string requestId, string replyName, bool byRequestId,
        TimeSpan timeout, CancellationToken ct, bool requireReady = true)
    {
        SemaphoreSlim? gate = byRequestId ? null : Gate(replyName);
        if (gate != null) await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            using var w = AddWaiter(replyName, byRequestId ? requestId : null);
            await SendAsync(outer, msg, requestId, ct, requireReady).ConfigureAwait(false);
            return await w.Task.WaitAsync(timeout, ct).ConfigureAwait(false);
        }
        finally
        {
            gate?.Release();
        }
    }

    public void Dispose()
    {
        _loopCts?.Cancel();
        _http.Dispose();
    }
}
