using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

internal static class Harness
{
    public static List<Rule> ConnectRules(JsonObject f) => new()
    {
        new Rule("authenticate", new() { JsonNode.Parse("{\"name\":\"authenticated\",\"msg\":true,\"request_id\":\"$RID\"}")!.AsObject() }),
        new Rule("sendMessage:get-profile", new() { f["profile"]!.DeepClone().AsObject() }),
        new Rule("sendMessage:get-balances", new() { f["balances"]!.DeepClone().AsObject(), f["time_sync"]!.DeepClone().AsObject() }),
    };

    public static IqOptionOptions Options(JsonObject f) => new()
    {
        TimeZone = Golden.Zone((string)f["tz"]!),
        RequestTimeout = TimeSpan.FromSeconds(5),
        AutoReconnect = false,
    };

    public static async Task<(IqOption Api, FakeSocket Socket, FakeHttpHandler Http)> ConnectedAsync(JsonObject fixtures)
    {
        var socket = new FakeSocket { Rules = ConnectRules(fixtures) };
        var http = new FakeHttpHandler();
        var api = new IqOption("user@example.com", "secret", Options(fixtures), () => socket, http);
        var (ok, reason) = await api.ConnectAsync();
        Assert.True(ok, reason);
        socket.TakeFrames();
        return (api, socket, http);
    }

    public static async Task SyncAsync(IqOption api, FakeSocket socket)
    {
        using var w = api.Conn.AddWaiter("__sync__", null);
        socket.Push("{\"name\":\"__sync__\"}");
        await w.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }
}
