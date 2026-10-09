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
        Assert.Equal(golden["frames"]!.AsArray().Select(x => (string)x!).ToArray(), socket.TakeFrames());
        var login = http.Requests.Single(r => r.Url.EndsWith("/api/v2/login", StringComparison.Ordinal));
        Assert.Equal((string)golden["login_url"]!, login.Url);
        var form = login.Body.Split('&').Select(p => p.Split('=')).ToDictionary(p => p[0], p => Uri.UnescapeDataString(p[1].Replace('+', ' ')));
        Assert.Equal(golden["login_form"]!.AsObject().ToDictionary(p => p.Key, p => (string)p.Value!), form);
        Assert.Equal((long)f["practice_id"]!, api.StateForTests.BalanceId);
        Assert.Equal((double)(long)f["T"]!, api.GetServerTimestamp());
        Assert.True(api.IsConnected);
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
