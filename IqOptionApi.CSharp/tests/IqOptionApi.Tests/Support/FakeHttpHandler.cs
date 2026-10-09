using System.Net;

namespace IqOptionApi.Tests;

internal sealed class FakeHttpHandler : HttpMessageHandler
{
    public List<(string Method, string Url, string Body, string Headers)> Requests { get; } = new();

    public Func<HttpRequestMessage, string, HttpResponseMessage>? Respond { get; set; }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        string body = request.Content == null ? "" : await request.Content.ReadAsStringAsync(ct);
        lock (Requests) Requests.Add((request.Method.Method, request.RequestUri!.ToString(), body, request.Headers.ToString()));
        if (Respond != null) return Respond(request, body);
        var resp = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{\"code\":\"success\"}") };
        if (request.RequestUri!.AbsolutePath.EndsWith("/api/v2/login", StringComparison.Ordinal))
            resp.Headers.Add("Set-Cookie", "ssid=SSID-TEST; Path=/; HttpOnly");
        return resp;
    }
}
