using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

/// <summary>Perpetual crypto (marginal-crypto). Frames and pushes are trimmed copies of what the official web traderoom
/// sent and received for a Bitcoin-PerpFuture practice trade (captured Oct 2026); Python has no equivalent.</summary>
public class PerpetualTests
{
    private const long OrderId = 100990310466;
    private const long PositionId = 100105468168;

    private const string Opened =
        "{\"name\":\"position-changed\",\"microserviceName\":\"portfolio\",\"msg\":{\"id\":\"f33ad1ede97748e57d4ee4aa09ea18b1\",\"external_id\":100105468168,"
        + "\"active_id\":1979,\"source\":\"marginal-crypto\",\"instrument_type\":\"marginal-crypto\",\"status\":\"open\",\"open_quote\":83063.5,\"invest\":8.31,"
        + "\"raw_event\":{\"marginal_crypto_position_changed1\":{\"id\":100105468168,\"status\":\"open\",\"type\":\"long\",\"count\":\"0.1\",\"leverage\":\"1000\","
        + "\"order_ids\":[100990310466],\"change_reason\":\"CREATED\"}}}}";

    private const string Closed =
        "{\"name\":\"position-changed\",\"microserviceName\":\"portfolio\",\"msg\":{\"id\":\"f33ad1ede97748e57d4ee4aa09ea18b1\",\"external_id\":100105468168,"
        + "\"active_id\":1979,\"source\":\"marginal-crypto\",\"instrument_type\":\"marginal-crypto\",\"status\":\"closed\",\"close_reason\":\"default\","
        + "\"close_profit\":0.95,\"pnl\":0.95,\"pnl_realized\":0.95,\"pnl_net\":0.95,\"invest\":8.31,"
        + "\"raw_event\":{\"marginal_crypto_position_changed1\":{\"id\":100105468168,\"status\":\"closed\",\"pnl_net\":\"0.95\","
        + "\"order_ids\":[100990310466,100990310493],\"change_reason\":\"CLOSED_DEFAULT\"}}}}";

    // The API session (our portfolio.position-changed 2.0 subscription) receives raw_event flat, not wrapped (live run, Oct 2026).
    private const string OpenedFlat =
        "{\"name\":\"position-changed\",\"microserviceName\":\"portfolio\",\"msg\":{\"id\":\"f33ad1ede97748e57d4ee4aa09ea18b1\",\"external_id\":100105468168,"
        + "\"active_id\":1979,\"source\":\"marginal-crypto\",\"instrument_type\":\"marginal-crypto\",\"status\":\"open\",\"invest\":1,"
        + "\"raw_event\":{\"index\":\"887532772\",\"id\":100105468168,\"status\":\"open\",\"type\":\"long\",\"leverage\":\"200\",\"close_reason\":null,"
        + "\"order_ids\":[100990310466],\"initial_margin\":\"1\",\"is_margin_isolated\":true,\"keep_position_open\":false,\"change_reason\":\"CREATED\"}}}";

    private static JsonObject Fixtures => Golden.Load("methods.json")["fixtures"]!.AsObject();

    private static JsonObject Msg(string frame) => JsonNode.Parse(frame)!["msg"]!.AsObject();

    [Fact]
    public async Task Open_SendsTheWebClientsOrderAndReturnsOrderAndPositionIds()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Fixtures);
        await using var _ = api;
        socket.Rules = new()
        {
            new Rule("sendMessage:marginal-crypto.place-market-order", new()
            {
                JsonNode.Parse("{\"request_id\":\"$RID\",\"name\":\"market-order-placed\",\"msg\":{\"id\":100990310466},\"status\":2000}")!.AsObject(),
                JsonNode.Parse(Opened)!.AsObject(),
            }),
        };

        var result = await api.OpenPerpetualAsync(1979, "buy", 0.1, 1000);

        Assert.Equal((true, (long?)OrderId, (long?)PositionId, (string?)null), result);
        var sent = Assert.Single(socket.TakeFrames());
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(
            "{\"name\":\"marginal-crypto.place-market-order\",\"version\":\"1.0\",\"body\":{\"side\":\"buy\",\"user_balance_id\":10004,"
            + "\"instrument_id\":\"mcrpt.1979\",\"instrument_active_id\":1979,\"leverage\":\"1000\",\"count\":\"0.1\",\"is_margin_isolated\":false}}"),
            Msg(sent)), sent);
    }

    [Fact]
    public async Task OpenIsolated_SendsTheWebClientsSimpleModeOrder()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Fixtures);
        await using var _ = api;
        socket.Rules = new()
        {
            new Rule("sendMessage:marginal-crypto.place-market-order", new()
            {
                JsonNode.Parse("{\"request_id\":\"$RID\",\"name\":\"market-order-placed\",\"msg\":{\"id\":100990310466},\"status\":2000}")!.AsObject(),
                JsonNode.Parse(Opened)!.AsObject(),
            }),
        };

        var result = await api.OpenPerpetualIsolatedAsync(1979, "buy", 1, 1000);

        Assert.Equal((true, (long?)OrderId, (long?)PositionId, (string?)null), result);
        var sent = Assert.Single(socket.TakeFrames());
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(
            "{\"name\":\"marginal-crypto.place-market-order\",\"version\":\"1.0\",\"body\":{\"side\":\"buy\",\"user_balance_id\":10004,"
            + "\"instrument_id\":\"mcrpt.1979\",\"instrument_active_id\":1979,\"leverage\":\"1000\",\"margin\":\"1\",\"is_margin_isolated\":true,"
            + "\"keep_position_open\":false}}"),
            Msg(sent)), sent);
    }

    [Fact]
    public async Task OpenIsolated_FractionalMarginIsPlainDecimalText_AndBadInputThrows()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Fixtures);
        await using var _ = api;
        socket.Rules = new()
        {
            new Rule("sendMessage:marginal-crypto.place-market-order", new()
            {
                JsonNode.Parse("{\"request_id\":\"$RID\",\"name\":\"market-order-placed\",\"msg\":{\"id\":100990310466},\"status\":2000}")!.AsObject(),
                JsonNode.Parse(Opened)!.AsObject(),
            }),
        };

        await api.OpenPerpetualIsolatedAsync(1979, "sell", 2.5, 200);
        Assert.Equal("2.5", (string)Msg(Assert.Single(socket.TakeFrames()))["body"]!["margin"]!);

        await Assert.ThrowsAsync<ArgumentException>(() => api.OpenPerpetualIsolatedAsync(1979, "short", 1, 200));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => api.OpenPerpetualIsolatedAsync(1979, "buy", 0, 200));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => api.OpenPerpetualIsolatedAsync(1979, "buy", 1, -5));
        Assert.Empty(socket.TakeFrames());
    }

    [Fact]
    public async Task Open_SmallCountIsSentAsPlainDecimalText()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Fixtures);
        await using var _ = api;
        socket.Rules = new()
        {
            new Rule("sendMessage:marginal-crypto.place-market-order", new()
            {
                JsonNode.Parse("{\"request_id\":\"$RID\",\"name\":\"market-order-placed\",\"msg\":{\"id\":100990310466},\"status\":2000}")!.AsObject(),
                JsonNode.Parse(Opened)!.AsObject(),
            }),
        };

        await api.OpenPerpetualAsync(1979, "sell", 0.00001, 200);

        var body = Msg(Assert.Single(socket.TakeFrames()))["body"]!;
        Assert.Equal("0.00001", (string)body["count"]!);
        Assert.Equal("200", (string)body["leverage"]!);
        Assert.Equal("sell", (string)body["side"]!);
    }

    [Fact]
    public async Task Open_RefusedByServer_ReturnsServerText()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Fixtures);
        await using var _ = api;
        socket.Rules = new()
        {
            new Rule("sendMessage:marginal-crypto.place-market-order", new()
            {
                JsonNode.Parse("{\"request_id\":\"$RID\",\"name\":\"market-order-placed\",\"msg\":{\"message\":\"not enough money\"},\"status\":4000}")!.AsObject(),
            }),
        };

        var (ok, orderId, positionId, error) = await api.OpenPerpetualAsync(1979, "buy", 0.1, 1000);

        Assert.False(ok);
        Assert.Null(orderId);
        Assert.Null(positionId);
        Assert.Equal("not enough money", error);
    }

    [Fact]
    public async Task Open_PlacedButNoPosition_KeepsTheOrderId()
    {
        var f = Fixtures;
        var socket = new FakeSocket { Rules = Harness.ConnectRules(f) };
        var options = Harness.Options(f);
        options.RequestTimeout = TimeSpan.FromMilliseconds(300);
        await using var api = new IqOption("user@example.com", "secret", options, () => socket, new FakeHttpHandler());
        Assert.True((await api.ConnectAsync()).Success);
        socket.Rules = new()
        {
            new Rule("sendMessage:marginal-crypto.place-market-order", new()
            {
                JsonNode.Parse("{\"request_id\":\"$RID\",\"name\":\"market-order-placed\",\"msg\":{\"id\":100990310466},\"status\":2000}")!.AsObject(),
            }),
        };

        var (ok, orderId, positionId, error) = await api.OpenPerpetualAsync(1979, "buy", 0.1, 1000);

        Assert.False(ok);
        Assert.Equal(OrderId, orderId);
        Assert.Null(positionId);
        Assert.NotNull(error);
    }

    [Fact]
    public async Task Open_RejectedOrder_ReturnsRejectStatus()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Fixtures);
        await using var _ = api;
        socket.Rules = new()
        {
            new Rule("sendMessage:marginal-crypto.place-market-order", new()
            {
                JsonNode.Parse("{\"request_id\":\"$RID\",\"name\":\"market-order-placed\",\"msg\":{\"id\":100990310466},\"status\":2000}")!.AsObject(),
                JsonNode.Parse("{\"name\":\"order-changed\",\"microserviceName\":\"portfolio\",\"msg\":{\"source\":\"marginal-crypto\",\"status\":\"rejected\","
                    + "\"instrument_type\":\"marginal-crypto\",\"raw_event\":{\"marginal_crypto_order_changed1\":{\"id\":100990310466,\"status\":\"rejected\","
                    + "\"side\":\"buy\",\"position_id\":null,\"reject_status\":\"insufficient_funds\"}}}}")!.AsObject(),
            }),
        };

        var result = await api.OpenPerpetualAsync(1979, "buy", 0.1, 1000);

        Assert.Equal((false, (long?)OrderId, (long?)null, (string?)"insufficient_funds"), result);
    }

    [Fact]
    public async Task Open_BadSideOrSize_ThrowsAndSendsNothing()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Fixtures);
        await using var _ = api;
        await Assert.ThrowsAsync<ArgumentException>(() => api.OpenPerpetualAsync(1979, "long", 0.1, 1000));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => api.OpenPerpetualAsync(1979, "buy", 0, 1000));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => api.OpenPerpetualAsync(1979, "buy", 0.1, 0));
        Assert.Empty(socket.TakeFrames());
    }

    [Fact]
    public async Task Close_SendsTheWebClientsFrame()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Fixtures);
        await using var _ = api;
        socket.Rules = new()
        {
            new Rule("sendMessage:marginal-crypto.close-position", new()
            {
                JsonNode.Parse("{\"request_id\":\"$RID\",\"name\":\"position-closed\",\"msg\":{\"id\":100105468168},\"status\":2000}")!.AsObject(),
            }),
        };

        Assert.True(await api.ClosePerpetualAsync(PositionId));

        var sent = Assert.Single(socket.TakeFrames());
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(
            "{\"name\":\"marginal-crypto.close-position\",\"version\":\"1.0\",\"body\":{\"position_id\":100105468168}}"), Msg(sent)), sent);
    }

    [Fact]
    public async Task Close_Refused_ReturnsFalse()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Fixtures);
        await using var _ = api;
        socket.Rules = new()
        {
            new Rule("sendMessage:marginal-crypto.close-position", new()
            {
                JsonNode.Parse("{\"request_id\":\"$RID\",\"name\":\"position-closed\",\"msg\":{\"message\":\"position not found\"},\"status\":4004}")!.AsObject(),
            }),
        };

        Assert.False(await api.ClosePerpetualAsync(PositionId));
    }

    [Fact]
    public async Task CheckWin_ReturnsPnlNetOfTheClosedPosition()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Fixtures);
        await using var _ = api;
        socket.Push(Opened);
        var check = api.CheckWinPerpetualAsync(PositionId);
        await Harness.SyncAsync(api, socket);
        Assert.False(check.IsCompleted);

        socket.Push(Closed);
        var (result, profit) = await check.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(TradeResult.Win, result);
        Assert.Equal(0.95, profit, 9);
    }

    [Fact]
    public async Task Open_UnderstandsTheFlatRawEventTheApiSessionReceives()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Fixtures);
        await using var _ = api;
        socket.Rules = new()
        {
            new Rule("sendMessage:marginal-crypto.place-market-order", new()
            {
                JsonNode.Parse("{\"request_id\":\"$RID\",\"name\":\"market-order-placed\",\"msg\":{\"id\":100990310466},\"status\":2000}")!.AsObject(),
                JsonNode.Parse(OpenedFlat)!.AsObject(),
            }),
        };

        var result = await api.OpenPerpetualIsolatedAsync(1979, "buy", 1, 200);

        Assert.Equal((true, (long?)OrderId, (long?)PositionId, (string?)null), result);
    }

    [Fact]
    public async Task Open_FlatRejectedOrderChanged_ReturnsRejectStatus()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Fixtures);
        await using var _ = api;
        socket.Rules = new()
        {
            new Rule("sendMessage:marginal-crypto.place-market-order", new()
            {
                JsonNode.Parse("{\"request_id\":\"$RID\",\"name\":\"market-order-placed\",\"msg\":{\"id\":100990310466},\"status\":2000}")!.AsObject(),
                JsonNode.Parse("{\"name\":\"order-changed\",\"microserviceName\":\"portfolio\",\"msg\":{\"source\":\"marginal-crypto\",\"status\":\"rejected\","
                    + "\"instrument_type\":\"marginal-crypto\",\"raw_event\":{\"id\":100990310466,\"status\":\"rejected\",\"side\":\"buy\",\"position_id\":null,"
                    + "\"reject_status\":\"insufficient_funds\"}}}")!.AsObject(),
            }),
        };

        var result = await api.OpenPerpetualAsync(1979, "buy", 0.1, 1000);

        Assert.Equal((false, (long?)OrderId, (long?)null, (string?)"insufficient_funds"), result);
    }

    [Fact]
    public async Task PositionChanged_IsStoredUnderOrderAndPositionIds()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Fixtures);
        await using var _ = api;
        socket.Push(Opened);
        await Harness.SyncAsync(api, socket);

        Assert.Equal("open", (string)api.GetAsyncOrder(OrderId)["position-changed"]!["msg"]!["status"]!);
        Assert.Equal("open", (string)api.GetAsyncOrder(PositionId)["position-changed"]!["msg"]!["status"]!);
    }
}
