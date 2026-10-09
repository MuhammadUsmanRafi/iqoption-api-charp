namespace IqOptionApi.Tests;

public class ConnectGoldenTests
{
    [Fact]
    public async Task ConnectSendsTheSameFramesAsPython()
    {
        var golden = Golden.Load("connect.json");
        var f = golden["fixtures"]!.AsObject();
        var socket = new FakeSocket { Rules = Harness.ConnectRules(f) };
        var http = new FakeHttpHandler();
        await using var api = new IqOption("user@example.com", "secret", Harness.Options(f), () => socket, http);

        var (ok, reason) = await api.ConnectAsync();

        Assert.True(ok, reason);
        // Blitz and perpetual-crypto subscriptions are our additions (Python never subscribes them); everything else is Python's sequence.
        Assert.Equal(golden["frames"]!.AsArray().Select(x => (string)x!).ToArray(),
            socket.TakeFrames().Where(x => !x.Contains("\"blitz-option\"", StringComparison.Ordinal) && !x.Contains("\"marginal-crypto\"", StringComparison.Ordinal)).ToArray());
        var login = http.Requests.Single(r => r.Url.EndsWith("/api/v2/login", StringComparison.Ordinal));
        Assert.Equal((string)golden["login_url"]!, login.Url);
        var form = login.Body.Split('&').Select(p => p.Split('=')).ToDictionary(p => p[0], p => Uri.UnescapeDataString(p[1].Replace('+', ' ')));
        Assert.Equal(golden["login_form"]!.AsObject().ToDictionary(p => p.Key, p => (string)p.Value!), form);
        Assert.Equal((long)f["practice_id"]!, api.StateForTests.BalanceId);
        Assert.Equal((double)(long)f["T"]!, api.GetServerTimestamp());
        Assert.True(api.IsConnected);
    }

    [Fact]
    public async Task Connect_SubscribesBlitzPositionsForEveryBalance()
    {
        // The live server only pushes blitz position-changed (open and closed) to clients that subscribe to "blitz-option";
        // without it CheckWinAsync never sees a blitz option close.
        var f = Golden.Load("connect.json")["fixtures"]!.AsObject();
        var socket = new FakeSocket { Rules = Harness.ConnectRules(f) };
        await using var api = new IqOption("user@example.com", "secret", Harness.Options(f), () => socket, new FakeHttpHandler());

        var (ok, reason) = await api.ConnectAsync();

        Assert.True(ok, reason);
        var blitz = socket.TakeFrames().Where(x => x.Contains("\"blitz-option\"", StringComparison.Ordinal)).ToArray();
        Assert.Equal(new[] { 10001L, 10004L }.Select(id =>
            "{\"name\": \"subscribeMessage\", \"msg\": {\"name\": \"portfolio.position-changed\", \"version\": \"2.0\", \"params\": {\"routingFilters\": "
            + $"{{\"instrument_type\": \"blitz-option\", \"user_balance_id\": {id}}}}}}}, \"request_id\": \"\"}}").ToArray(), blitz);
    }

    [Fact]
    public async Task Connect_SubscribesPerpetualCryptoPositionsAndOrders()
    {
        // The web traderoom receives marginal-crypto position-changed / order-changed; we subscribe like the other portfolio types.
        var f = Golden.Load("connect.json")["fixtures"]!.AsObject();
        var socket = new FakeSocket { Rules = Harness.ConnectRules(f) };
        await using var api = new IqOption("user@example.com", "secret", Harness.Options(f), () => socket, new FakeHttpHandler());

        var (ok, reason) = await api.ConnectAsync();

        Assert.True(ok, reason);
        var frames = socket.TakeFrames().Where(x => x.Contains("\"marginal-crypto\"", StringComparison.Ordinal)).ToArray();
        Assert.Equal(new[] { 10001L, 10004L }.Select(id =>
            "{\"name\": \"subscribeMessage\", \"msg\": {\"name\": \"portfolio.position-changed\", \"version\": \"2.0\", \"params\": {\"routingFilters\": "
            + $"{{\"instrument_type\": \"marginal-crypto\", \"user_balance_id\": {id}}}}}}}, \"request_id\": \"\"}}")
            .Append("{\"name\": \"subscribeMessage\", \"msg\": {\"name\": \"portfolio.order-changed\", \"version\": \"1.0\", \"params\": {\"routingFilters\": "
            + "{\"instrument_type\": \"marginal-crypto\"}}}, \"request_id\": \"\"}").ToArray(), frames);
    }

    [Fact]
    public async Task WrongPassword_ReturnsServerText()
    {
        var f = Golden.Load("connect.json")["fixtures"]!.AsObject();
        var http = new FakeHttpHandler
        {
            Respond = (_, _) => new HttpResponseMessage(System.Net.HttpStatusCode.BadRequest)
            {
                Content = new StringContent("{\"code\":\"invalid_credentials\"}"),
            },
        };
        await using var api = new IqOption("user@example.com", "bad", Harness.Options(f), () => new FakeSocket(), http);
        var (ok, reason) = await api.ConnectAsync();
        Assert.False(ok);
        Assert.Equal("{\"code\":\"invalid_credentials\"}", reason);
    }

    [Fact]
    public async Task CallsBeforeConnect_Throw()
    {
        await using var api = new IqOption("a", "b");
        await Assert.ThrowsAsync<InvalidOperationException>(() => api.GetProfileAsync());
    }
}
