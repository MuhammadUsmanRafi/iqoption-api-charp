using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

public class MethodGoldenTests
{
    private static readonly Lazy<JsonObject> Data = new(() => Golden.Load("methods.json"));

    public static IEnumerable<object[]> Cases() =>
        Data.Value["cases"]!.AsArray().Select(c => new object[] { (string)c!["name"]! });

    [Theory, MemberData(nameof(Cases))]
    public async Task MatchesPython(string name)
    {
        var c = Data.Value["cases"]!.AsArray().Select(x => x!.AsObject()).First(x => (string)x["name"]! == name);
        var (api, socket, _) = await Harness.ConnectedAsync(Data.Value["fixtures"]!.AsObject());
        await using var _ = api;
        socket.Rules = Rule.Parse(c["rules"]);
        foreach (var m in c["pre"]!.AsArray()) socket.Push(m!.ToJsonString());
        await Harness.SyncAsync(api, socket);
        Calls.Setup(api, (string?)c["setup_csharp"]);
        var call = c["csharp"]!.AsObject();

        var result = await Calls.InvokeAsync(api, socket, (string)call["method"]!, call["args"]?.AsObject() ?? new JsonObject());

        Assert.Equal(c["frames"]!.AsArray().Select(f => (string)f!).ToArray(), socket.TakeFrames());
        if ((bool)c["compare_result"]!) JsonAssert.Equal(c["result"], result);
    }
}
