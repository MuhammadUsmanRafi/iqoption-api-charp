using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace IqOptionApi.Tests;

/// <summary>Small in-process IQ Option look-alike: HTTP login/logout and a WebSocket that answers by <see cref="Rule"/>s.</summary>
internal sealed class FakeIqServer : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly List<Rule> _rules = new();
    private WebSocket? _current;
    private int _logins;
    private int _connections;
    private readonly ConcurrentDictionary<string, bool> _dropOn = new();

    private FakeIqServer(WebApplication app) => _app = app;

    public string BaseUrl { get; private set; } = "";

    public ConcurrentQueue<string> Frames { get; } = new();

    public int Logins => Volatile.Read(ref _logins);

    public int Connections => Volatile.Read(ref _connections);

    public bool RejectLogin { get; set; }

    public static async Task<FakeIqServer> StartAsync(JsonObject fixtures)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        var app = builder.Build();
        var server = new FakeIqServer(app);
        server._rules.AddRange(Harness.ConnectRules(fixtures));
        app.UseWebSockets();
        app.Run(server.HandleAsync);
        await app.StartAsync();
        server.BaseUrl = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return server;
    }

    public void AddRule(string on, params string[] replies)
    {
        lock (_rules) _rules.Add(new Rule(on, replies.Select(r => JsonNode.Parse(r)!.AsObject()).ToList()));
    }

    public void ReplaceRule(string on, params string[] replies)
    {
        lock (_rules) _rules.RemoveAll(r => r.On == on);
        AddRule(on, replies);
    }

    public string[] FramesNamed(string key) => Frames.Where(f => Rule.KeyOf(JsonNode.Parse(f)!.AsObject()) == key).ToArray();

    public void Drop() => _current?.Abort();

    /// <summary>Abort the socket (without answering) the next time a frame with this key arrives.</summary>
    public void DropOnNext(string key) => _dropOn[key] = true;

    public async Task PushAsync(string json)
    {
        var ws = _current;
        if (ws != null) await SendAsync(ws, json);
    }

    private async Task HandleAsync(HttpContext ctx)
    {
        string path = ctx.Request.Path.Value ?? "";
        if (path == "/api/v2/login")
        {
            int n = Interlocked.Increment(ref _logins);
            if (RejectLogin)
            {
                ctx.Response.StatusCode = 400;
                await ctx.Response.WriteAsync("{\"code\":\"invalid_credentials\"}");
                return;
            }
            ctx.Response.Headers.Append("Set-Cookie", $"ssid=SSID-{n}; Path=/");
            await ctx.Response.WriteAsync("{\"code\":\"success\"}");
        }
        else if (path == "/api/v1.0/logout")
        {
            await ctx.Response.WriteAsync("{}");
        }
        else if (path == "/echo/websocket" && ctx.WebSockets.IsWebSocketRequest)
        {
            using var ws = await ctx.WebSockets.AcceptWebSocketAsync();
            _current = ws;
            Interlocked.Increment(ref _connections);
            await ServeAsync(ws, ctx.RequestAborted);
        }
        else
        {
            ctx.Response.StatusCode = 404;
        }
    }

    private async Task ServeAsync(WebSocket ws, CancellationToken ct)
    {
        var buffer = new byte[64 * 1024];
        try
        {
            while (ws.State == WebSocketState.Open)
            {
                using var ms = new MemoryStream();
                WebSocketReceiveResult r;
                do
                {
                    r = await ws.ReceiveAsync(buffer, ct);
                    if (r.MessageType == WebSocketMessageType.Close) return;
                    ms.Write(buffer, 0, r.Count);
                }
                while (!r.EndOfMessage);
                string text = Encoding.UTF8.GetString(ms.ToArray());
                Frames.Enqueue(text);
                if (_dropOn.TryRemove(Rule.KeyOf(JsonNode.Parse(text)!.AsObject()), out _))
                {
                    ws.Abort();
                    return;
                }
                List<Rule> rules;
                lock (_rules) rules = _rules.ToList();
                foreach (string reply in Rule.RepliesFor(JsonNode.Parse(text)!.AsObject(), rules)) await SendAsync(ws, reply);
            }
        }
        catch (Exception ex) when (ex is WebSocketException or OperationCanceledException or ObjectDisposedException)
        {
            // the client went away or the test aborted the socket
        }
    }

    private async Task SendAsync(WebSocket ws, string text)
    {
        await _sendLock.WaitAsync();
        try
        {
            await ws.SendAsync(Encoding.UTF8.GetBytes(text), WebSocketMessageType.Text, true, CancellationToken.None);
        }
        catch (Exception ex) when (ex is WebSocketException or ObjectDisposedException)
        {
            // socket already gone
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
