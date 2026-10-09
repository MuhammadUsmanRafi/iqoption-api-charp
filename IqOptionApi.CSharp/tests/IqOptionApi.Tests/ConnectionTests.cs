using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

public class ConnectionTests
{
    private static (Connection Conn, FakeSocket Socket) Open(TimeSpan? timeout = null)
    {
        var socket = new FakeSocket();
        var options = new IqOptionOptions { RequestTimeout = timeout ?? TimeSpan.FromSeconds(2) };
        var conn = new Connection(options, () => socket, new FakeHttpHandler(), _ => { });
        conn.OpenAsync(default).GetAwaiter().GetResult();
        conn.Ready = true;
        return (conn, socket);
    }

    [Fact]
    public async Task RequestIds_StartAtOneAndResetOnOpen()
    {
        var (conn, _) = Open();
        Assert.Equal("1", conn.NextRequestId());
        Assert.Equal("2", conn.NextRequestId());
        await conn.OpenAsync(default);
        Assert.Equal("1", conn.NextRequestId());
    }

    [Fact]
    public async Task RequestById_MatchesOnlyItsOwnRequestId()
    {
        var (conn, socket) = Open();
        var a = conn.RequestAsync("sendMessage", new Obj { ["name"] = "x" }, "7", "option", true, TimeSpan.FromSeconds(2), default);
        var b = conn.RequestAsync("sendMessage", new Obj { ["name"] = "x" }, "8", "option", true, TimeSpan.FromSeconds(2), default);
        socket.Push("{\"name\":\"option\",\"request_id\":\"8\",\"msg\":{\"id\":2}}");
        socket.Push("{\"name\":\"option\",\"request_id\":\"7\",\"msg\":{\"id\":1}}");
        Assert.Equal(1, (int)(await a)["msg"]!["id"]!);
        Assert.Equal(2, (int)(await b)["msg"]!["id"]!);
        Assert.Equal("{\"name\": \"sendMessage\", \"msg\": {\"name\": \"x\"}, \"request_id\": \"7\"}", socket.Frames[0]);
    }

    [Fact]
    public async Task RequestByName_SerialisesSameReplyName()
    {
        var (conn, socket) = Open();
        var first = conn.RequestAsync("sendMessage", new Obj { ["name"] = "get-positions" }, "", "positions", false, TimeSpan.FromSeconds(2), default);
        var second = conn.RequestAsync("sendMessage", new Obj { ["name"] = "portfolio.get-positions" }, "", "positions", false, TimeSpan.FromSeconds(2), default);
        await Task.Delay(100);
        Assert.Single(socket.Frames);   // second waits for the first answer before sending
        socket.Push("{\"name\":\"positions\",\"msg\":{\"n\":1}}");
        Assert.Equal(1, (int)(await first)["msg"]!["n"]!);
        await Task.Delay(100);
        Assert.Equal(2, socket.Frames.Count);
        socket.Push("{\"name\":\"positions\",\"msg\":{\"n\":2}}");
        Assert.Equal(2, (int)(await second)["msg"]!["n"]!);
    }

    [Fact]
    public async Task RequestAsync_TimesOut()
    {
        var (conn, socket) = Open();
        socket.Push("{\"name\":\"result\",\"request_id\":\"5\",\"msg\":{\"success\":false}}");
        await Assert.ThrowsAsync<TimeoutException>(() =>
            conn.RequestAsync("sendMessage", new Obj(), "5", "option", true, TimeSpan.FromMilliseconds(200), default));
    }

    [Fact]
    public async Task Drop_FailsWaitersWithConnectionLost_AndRaisesClosed()
    {
        var (conn, socket) = Open();
        string? closed = null;
        conn.Closed += r => closed = r;
        var pending = conn.RequestAsync("sendMessage", new Obj(), "3", "option", true, TimeSpan.FromSeconds(5), default);
        socket.Drop();
        var ex = await Assert.ThrowsAsync<IqOptionException>(() => pending);
        Assert.Equal("connection lost", ex.Message);
        await Task.Delay(50);
        Assert.NotNull(closed);
        Assert.False(conn.IsOpen);
    }

    [Fact]
    public async Task WaitUntil_WakesOnMessages()
    {
        var (conn, socket) = Open();
        int seen = 0;
        conn.MessageReceived += _ => Interlocked.Increment(ref seen);
        var wait = conn.WaitUntilAsync(() => Volatile.Read(ref seen) >= 2, TimeSpan.FromSeconds(2), default);
        socket.Push("{\"name\":\"a\"}");
        socket.Push("{\"name\":\"b\"}");
        await wait;
        await Assert.ThrowsAsync<TimeoutException>(() => conn.WaitUntilAsync(() => false, TimeSpan.FromMilliseconds(100), default));
    }

    [Fact]
    public async Task GarbageMessage_IsSkipped()
    {
        var (conn, socket) = Open();
        socket.Push("not json");
        var reply = conn.RequestAsync("sendMessage", new Obj(), "1", "option", true, TimeSpan.FromSeconds(2), default);
        socket.Push("{\"name\":\"option\",\"request_id\":\"1\",\"msg\":{}}");
        await reply;
    }

    [Fact]
    public async Task Login_SendsFormAndReadsSsidCookie()
    {
        var http = new FakeHttpHandler();
        var conn = new Connection(new IqOptionOptions(), () => new FakeSocket(), http, _ => { });
        var (status, _, ssid) = await conn.LoginAsync("a@b.com", "p w", null, default);
        Assert.Equal(200, status);
        Assert.Equal("SSID-TEST", ssid);
        Assert.Equal("https://auth.iqoption.com/api/v2/login", http.Requests[0].Url);
        Assert.Equal("identifier=a%40b.com&password=p+w", http.Requests[0].Body);
        Assert.Contains("Chrome/66.0.3359.139", http.Requests[0].Headers);
        await conn.LogoutAsync(default);
        Assert.Contains("ssid=SSID-TEST", http.Requests[1].Headers);
    }
}
