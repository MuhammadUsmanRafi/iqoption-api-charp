using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

/// <summary>Regression tests for the findings of the final code review.</summary>
public class ReviewFixTests
{
    private static JsonObject Fixtures => Golden.Load("methods.json")["fixtures"]!.AsObject();

    private static IqOption Client(FakeIqServer server, TimeSpan? reconnectDelay = null) => new("user@example.com", "secret", new IqOptionOptions
    {
        WebSocketUrl = server.BaseUrl.Replace("http://", "ws://", StringComparison.Ordinal) + "/echo/websocket",
        AuthUrl = server.BaseUrl,
        Host = "127.0.0.1",
        RequestTimeout = TimeSpan.FromSeconds(3),
        ReconnectDelays = new[] { reconnectDelay ?? TimeSpan.FromMilliseconds(50) },
        TimeZone = Golden.Zone("Asia/Karachi"),
    });

    private static async Task<(IqOption Api, FakeSocket Socket)> FakeConnectedAsync(TimeSpan timeout)
    {
        var f = Fixtures;
        var socket = new FakeSocket { Rules = Harness.ConnectRules(f) };
        var options = Harness.Options(f);
        options.RequestTimeout = timeout;
        var api = new IqOption("user@example.com", "secret", options, () => socket, new FakeHttpHandler());
        Assert.True((await api.ConnectAsync()).Success);
        socket.TakeFrames();
        return (api, socket);
    }

    private static Rule R(string on, string reply) => new(on, new() { JsonNode.Parse(reply)!.AsObject() });

    private static async Task WaitForAsync(Func<bool> condition)
    {
        for (int i = 0; i < 250 && !condition(); i++) await Task.Delay(20);
        Assert.True(condition(), "condition not reached in 5 s");
    }

    // Critical 1: a buy made during a reconnect is sent once, on the new socket, and its answer is returned.
    [Fact]
    public async Task BuyDuringReconnect_IsSentOnceAndSucceeds()
    {
        await using var server = await FakeIqServer.StartAsync(Fixtures);
        server.AddRule("sendMessage:binary-options.open-option", "{\"name\":\"option\",\"request_id\":\"$RID\",\"msg\":{\"id\":42}}");
        await using var api = Client(server, TimeSpan.FromMilliseconds(300));
        Assert.True((await api.ConnectAsync()).Success);

        server.Drop();
        await WaitForAsync(() => !api.IsConnected);
        var result = await api.BuyAsync(1, "EURUSD", Direction.Call, 1).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal((true, 42L, (string?)null), result);
        Assert.Single(server.FramesNamed("sendMessage:binary-options.open-option"));
    }

    // Critical 2: a second drop while results are being recovered still ends in a working connection.
    [Fact]
    public async Task SecondDropDuringRecovery_StillReconnects()
    {
        await using var server = await FakeIqServer.StartAsync(Fixtures);
        server.AddRule("sendMessage:get-history-positions",
            "{\"name\":\"history-positions\",\"request_id\":\"$RID\",\"msg\":{\"positions\":[{\"external_id\":42,\"pnl_realized\":0.8}]}}");
        await using var api = Client(server);
        Assert.True((await api.ConnectAsync()).Success);

        var check = api.CheckWinAsync(42);
        await Task.Delay(100);
        server.DropOnNext("sendMessage:get-history-positions");
        server.Drop();

        var (result, profit) = await check.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(TradeResult.Win, result);
        Assert.Equal(0.8, profit, 9);
        await WaitForAsync(() => api.IsConnected);
        Assert.Equal(3, server.Connections);
    }

    // Important 3: a connection that goes silent is treated as dropped.
    [Fact]
    public async Task SilentConnection_IsDetected()
    {
        var f = Fixtures;
        var socket = new FakeSocket { Rules = Harness.ConnectRules(f) };
        var options = Harness.Options(f);
        options.ReceiveTimeout = TimeSpan.FromMilliseconds(400);
        await using var api = new IqOption("user@example.com", "secret", options, () => socket, new FakeHttpHandler());
        var reasons = new List<string>();
        api.Disconnected += r => { lock (reasons) reasons.Add(r); };
        Assert.True((await api.ConnectAsync()).Success);

        await WaitForAsync(() => { lock (reasons) return reasons.Count > 0; });
        Assert.Contains("no data", reasons[0], StringComparison.Ordinal);
        Assert.False(api.IsConnected);
    }

    // Important 4: once the server gave an order id, a later failure still returns it.
    [Fact]
    public async Task BuyOrder_PlacedButStatusUnknown_ReturnsOrderId()
    {
        var (api, socket) = await FakeConnectedAsync(TimeSpan.FromMilliseconds(300));
        await using var _ = api;
        socket.Rules = new() { R("sendMessage:place-order-temp", "{\"name\":\"order-placed-temp\",\"request_id\":\"$RID\",\"msg\":{\"id\":4242}}") };

        var (ok, orderId, error) = await api.BuyOrderAsync("forex", "EURUSD", "buy", 10, 100, "market");

        Assert.False(ok);
        Assert.Equal(4242L, orderId);
        Assert.NotNull(error);
    }

    [Fact]
    public async Task BuyDigitalSpot_PlacedButNoPosition_ReturnsOrderId()
    {
        var (api, socket) = await FakeConnectedAsync(TimeSpan.FromMilliseconds(300));
        await using var _ = api;
        socket.Rules = new() { R("sendMessage:digital-options.place-digital-option", "{\"name\":\"digital-option-placed\",\"request_id\":\"$RID\",\"msg\":{\"id\":555}}") };

        var (ok, id, error) = await api.BuyDigitalSpotAsync("EURUSD", 1, Direction.Call, 1);

        Assert.False(ok);
        Assert.Equal(555L, id);
        Assert.Equal("timeout", error);
    }

    // Important 5: CheckWinMarginAsync survives timeouts and does not fall back to the order id on a timeout.
    [Fact]
    public async Task CheckWinMargin_SurvivesTimeouts_AndUsesPositionId()
    {
        var (api, socket) = await FakeConnectedAsync(TimeSpan.FromMilliseconds(300));
        await using var _ = api;
        socket.Rules = new();
        var check = api.CheckWinMarginAsync(4242, 0.05);

        await WaitForAsync(() => { lock (socket.Frames) return socket.Frames.Any(f => f.Contains("get-order", StringComparison.Ordinal)); });
        socket.Rules = new() { R("sendMessage:get-order", "{\"name\":\"order\",\"request_id\":\"$RID\",\"status\":2000,\"msg\":{\"id\":4242,\"status\":\"filled\",\"position_id\":5151}}") };
        await WaitForAsync(() => { lock (socket.Frames) return socket.Frames.Any(f => f.Contains("get-history-positions", StringComparison.Ordinal)); });
        await Task.Delay(400);   // the first history request times out
        socket.Rules.Add(R("sendMessage:get-history-positions", "{\"name\":\"history-positions\",\"request_id\":\"$RID\",\"msg\":{\"positions\":[{\"external_id\":5151,\"pnl_realized\":2.675}]}}"));

        Assert.Equal(2.67, await check.WaitAsync(TimeSpan.FromSeconds(5)));
        lock (socket.Frames) Assert.DoesNotContain(socket.Frames, f => f.Contains("\"external_id\": 4242", StringComparison.Ordinal));
    }

    // Important 6: one buy failing to send does not lose the ids of the others.
    [Fact]
    public async Task BuyMulti_OneSendFails_OthersStillReported()
    {
        var (api, socket) = await FakeConnectedAsync(TimeSpan.FromMilliseconds(300));
        await using var _ = api;
        socket.Rules = new() { R("sendMessage:binary-options.open-option", "{\"name\":\"option\",\"request_id\":\"$RID\",\"msg\":{\"id\":12345}}") };
        socket.FailSend = frame => frame.Contains("\"active_id\": 76,", StringComparison.Ordinal);

        var ids = await api.BuyMultiAsync(new[] { 1.0, 2.0 }, new[] { "EURUSD", "EURUSD-OTC" }, new[] { Direction.Call, Direction.Put }, new[] { 1, 1 });

        Assert.Equal(new long?[] { 12345, null }, ids);
    }
}
