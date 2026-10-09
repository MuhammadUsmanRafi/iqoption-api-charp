using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

public class ReconnectTests
{
    private static JsonObject Fixtures => Golden.Load("methods.json")["fixtures"]!.AsObject();

    private static IqOption Client(FakeIqServer server) => new("user@example.com", "secret", new IqOptionOptions
    {
        WebSocketUrl = server.BaseUrl.Replace("http://", "ws://", StringComparison.Ordinal) + "/echo/websocket",
        AuthUrl = server.BaseUrl,
        Host = "127.0.0.1",
        RequestTimeout = TimeSpan.FromSeconds(3),
        ReconnectDelays = new[] { TimeSpan.FromMilliseconds(50) },
        TimeZone = Golden.Zone("Asia/Karachi"),
    });

    private static async Task WaitForAsync(Func<bool> condition)
    {
        for (int i = 0; i < 250 && !condition(); i++) await Task.Delay(20);
        Assert.True(condition(), "condition not reached in 5 s");
    }

    [Fact]
    public async Task RealSocket_ConnectBuyAndCheckWin()
    {
        await using var server = await FakeIqServer.StartAsync(Fixtures);
        server.AddRule("sendMessage:binary-options.open-option",
            "{\"name\":\"option\",\"request_id\":\"$RID\",\"msg\":{\"id\":42}}",
            "{\"name\":\"option-closed\",\"msg\":{\"option_id\":42,\"amount\":1,\"profit_amount\":1.8}}");
        await using var api = Client(server);
        Assert.True((await api.ConnectAsync()).Success);

        var (ok, id, _) = await api.BuyAsync(1, "EURUSD", Direction.Call, 1);
        var (result, profit) = await api.CheckWinAsync(id).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(ok);
        Assert.Equal(TradeResult.Win, result);
        Assert.Equal(0.8, profit, 9);
    }

    [Fact]
    public async Task Drop_DuringBuy_FailsAndDoesNotResend()
    {
        await using var server = await FakeIqServer.StartAsync(Fixtures);
        await using var api = Client(server);
        var reconnected = new TaskCompletionSource();
        api.Reconnected += () => reconnected.TrySetResult();
        Assert.True((await api.ConnectAsync()).Success);

        var buy = api.BuyAsync(1, "EURUSD", Direction.Call, 1);
        await WaitForAsync(() => server.FramesNamed("sendMessage:binary-options.open-option").Length == 1);
        server.Drop();

        Assert.Equal((false, 0L, "connection lost"), await buy.WaitAsync(TimeSpan.FromSeconds(5)));
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Task.Delay(200);
        Assert.Single(server.FramesNamed("sendMessage:binary-options.open-option"));
        Assert.True(api.IsConnected);
    }

    [Fact]
    public async Task Reconnect_ResubscribesStreams_AndKeepsSelectedBalance()
    {
        var f = Fixtures;
        await using var server = await FakeIqServer.StartAsync(f);
        server.AddRule("subscribeMessage:traders-mood-changed", "{\"name\":\"traders-mood-changed\",\"msg\":{\"asset_id\":1861,\"value\":0.5}}");
        await using var api = Client(server);
        var reconnected = new TaskCompletionSource();
        api.Reconnected += () => reconnected.TrySetResult();
        Assert.True((await api.ConnectAsync()).Success);
        Assert.True(await api.StartMoodStreamAsync("EURUSD"));
        await api.ChangeBalanceAsync(BalanceMode.Real);

        server.Drop();
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(2, server.FramesNamed("subscribeMessage:traders-mood-changed").Length);
        Assert.Equal(2, server.FramesNamed("setOptions").Length);
        Assert.Equal((long)f["real_id"]!, api.GetBalanceId());
    }

    [Fact]
    public async Task Reconnect_RecoversResultPushedWhileOffline()
    {
        await using var server = await FakeIqServer.StartAsync(Fixtures);
        server.AddRule("sendMessage:get-history-positions",
            "{\"name\":\"history-positions\",\"request_id\":\"$RID\",\"msg\":{\"positions\":[{\"external_id\":42,\"pnl_realized\":0.8}]}}");
        await using var api = Client(server);
        Assert.True((await api.ConnectAsync()).Success);

        var check = api.CheckWinAsync(42);
        await Task.Delay(100);
        server.Drop();

        var (result, profit) = await check.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(TradeResult.Win, result);
        Assert.Equal(0.8, profit, 9);
    }

    [Fact]
    public async Task Close_StopsReconnecting()
    {
        await using var server = await FakeIqServer.StartAsync(Fixtures);
        await using var api = Client(server);
        Assert.True((await api.ConnectAsync()).Success);

        await api.CloseAsync();
        server.Drop();
        await Task.Delay(500);

        Assert.Equal(1, server.Logins);
        Assert.Equal(1, server.Connections);
        Assert.False(api.IsConnected);
    }

    [Fact]
    public async Task LoginRejectedOnReconnect_GivesUp()
    {
        await using var server = await FakeIqServer.StartAsync(Fixtures);
        await using var api = Client(server);
        var reasons = new List<string>();
        api.Disconnected += r => { lock (reasons) reasons.Add(r); };
        Assert.True((await api.ConnectAsync()).Success);

        server.RejectLogin = true;
        server.ReplaceRule("authenticate", "{\"name\":\"authenticated\",\"msg\":false,\"request_id\":\"$RID\"}");
        server.Drop();

        await WaitForAsync(() => { lock (reasons) return reasons.Any(r => r.StartsWith("login failed", StringComparison.Ordinal)); });
        Assert.False(api.IsConnected);
    }
}
