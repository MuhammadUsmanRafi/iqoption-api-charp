namespace IqOptionApi.Tests;

public class SessionTests
{
    [Fact]
    public async Task TwoFactor_PostsJsonLikePython()
    {
        var http = new FakeHttpHandler();
        await using var api = new IqOption("a", "b", new IqOptionOptions(), () => new FakeSocket(), http);
        await api.TwoFactorAsync("tok", method: "sms");
        var r = http.Requests.Single();
        Assert.Equal("https://auth.iqoption.com/api/v2/verify/2fa", r.Url);
        Assert.Equal("{\"method\": \"sms\", \"token\": \"tok\", \"code\": null}", r.Body);
    }

    [Fact]
    public async Task TwoFactorToken_IsSentWithNextLogin()
    {
        var http = new FakeHttpHandler();
        await using var api = new IqOption("a@b.c", "p", new IqOptionOptions { RequestTimeout = TimeSpan.FromMilliseconds(200), AutoReconnect = false }, () => new FakeSocket(), http);
        api.Set2FAToken("T3");
        await api.ConnectAsync();
        Assert.EndsWith("&token=T3", http.Requests.First(r => r.Url.EndsWith("/login", StringComparison.Ordinal)).Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SetSession_ReplacesHeadersAndCookies()
    {
        var http = new FakeHttpHandler();
        await using var api = new IqOption("a", "b", new IqOptionOptions { AutoReconnect = false }, () => new FakeSocket(), http);
        api.SetSession(new Dictionary<string, string> { ["User-Agent"] = "MyBot/1" }, new Dictionary<string, string> { ["lang"] = "en" });
        await api.LogoutAsync();
        Assert.Contains("MyBot/1", http.Requests[0].Headers);
        Assert.Contains("lang=en", http.Requests[0].Headers);
    }
}
