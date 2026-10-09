namespace IqOptionApi.Tests;

public class BinaryTests
{
    [Fact]
    public async Task Buy_UnknownAsset_SendsNothing()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Golden.Load("methods.json")["fixtures"]!.AsObject());
        await using var _ = api;
        await Assert.ThrowsAsync<KeyNotFoundException>(() => api.BuyAsync(1, "EURUSDD", Direction.Call, 1));
        Assert.Empty(socket.TakeFrames());
    }

    [Fact]
    public async Task Buy_TimesOutWithFailureTuple()
    {
        var f = Golden.Load("methods.json")["fixtures"]!.AsObject();
        var socket = new FakeSocket { Rules = Harness.ConnectRules(f) };
        var options = Harness.Options(f);
        options.RequestTimeout = TimeSpan.FromMilliseconds(300);
        await using var api = new IqOption("user@example.com", "secret", options, () => socket, new FakeHttpHandler());
        Assert.True((await api.ConnectAsync()).Success);
        socket.Rules = new() { new Rule("sendMessage:binary-options.open-option", new() { System.Text.Json.Nodes.JsonNode.Parse("{\"name\":\"result\",\"request_id\":\"$RID\",\"msg\":{\"success\":false}}")!.AsObject() }) };
        Assert.Equal((false, 0L, "timeout"), await api.BuyAsync(1, "EURUSD", Direction.Call, 1));
    }

    [Fact]
    public async Task Blitz_InvalidDuration_Throws()
    {
        var (api, _, _) = await Harness.ConnectedAsync(Golden.Load("methods.json")["fixtures"]!.AsObject());
        await using var _ = api;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => api.BuyBlitzAsync(1, "EURUSD", Direction.Call, 30));
    }

    // Live server (Oct 2026) no longer sends option-closed; a closed option arrives as position-changed (captured from a real practice trade).
    [Fact]
    public async Task CheckWin_UnderstandsClosedPositionChanged()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Golden.Load("methods.json")["fixtures"]!.AsObject());
        await using var _ = api;
        var check = api.CheckWinAsync(14338336472);
        socket.Push("{\"name\":\"position-changed\",\"microserviceName\":\"portfolio\",\"msg\":{\"source\":\"binary-options\",\"status\":\"closed\",\"external_id\":14338336472,\"close_reason\":\"win\",\"close_profit\":1.87,\"invest\":1,\"pnl_realized\":0.8700000000000001,\"instrument_type\":\"turbo-option\",\"raw_event\":{\"option_id\":14338336472}}}");
        var (result, profit) = await check.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(TradeResult.Win, result);
        Assert.Equal(0.87, profit, 9);
    }
}
