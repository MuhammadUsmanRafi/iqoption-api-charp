using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

public class RouterTests
{
    private static (MessageRouter Router, State State, List<string> Log) Make()
    {
        var state = new State();
        var log = new List<string>();
        return (new MessageRouter(state, new ActivesTable(), log.Add), state, log);
    }

    [Fact]
    public void StateMatchesPython()
    {
        var golden = Golden.Load("router.json");
        var (router, state, _) = Make();
        foreach (var (asset, sizes) in golden["maxdict"]!.AsObject())
            foreach (var (size, max) in sizes!.AsObject())
                State.GetOrAdd(state.MaxDict, asset)[int.Parse(size)] = (int)max!;
        foreach (var m in golden["messages"]!.AsArray()) router.Handle(m!.DeepClone().AsObject());
        JsonAssert.Equal(golden["state"], StateDump.Of(state));
    }

    [Fact]
    public void BadMessages_AreSkipped_AndLaterMessagesStillWork()
    {
        var (router, state, log) = Make();
        router.Handle(JsonNode.Parse("{\"name\":\"candle-generated\",\"msg\":{\"active_id\":424242,\"size\":60,\"from\":1}}")!.AsObject());
        router.Handle(JsonNode.Parse("{\"name\":\"client-price-generated\",\"msg\":{\"asset_id\":1861,\"prices\":[]}}")!.AsObject());
        router.Handle(JsonNode.Parse("{\"name\":\"position-changed\",\"msg\":5}")!.AsObject());
        router.Handle(JsonNode.Parse("{\"name\":\"traders-mood-changed\",\"msg\":{\"asset_id\":1861,\"value\":0.4}}")!.AsObject());
        Assert.Equal(3, log.Count(l => l.StartsWith("skipped", StringComparison.Ordinal)));
        Assert.Equal(0.4, state.TradersMood[1861]);
    }

    [Fact]
    public void ThrowingHandler_DoesNotStopRouting()
    {
        var (router, state, log) = Make();
        router.MessageReceived += _ => throw new InvalidOperationException("boom");
        router.BalanceChanged += (_, _) => throw new InvalidOperationException("boom2");
        router.Handle(JsonNode.Parse("{\"name\":\"balance-changed\",\"msg\":{\"current_balance\":{\"id\":2,\"amount\":3.5}}}")!.AsObject());
        Assert.Equal(3.5, state.Balance[2]);
        Assert.Equal(2, log.Count(l => l.Contains("threw", StringComparison.Ordinal)));
    }

    [Fact]
    public void LiveDealBuffer_DropsOldestWhenFull()
    {
        var b = new LiveDealBuffer { MaxLength = 2 };
        for (int i = 0; i < 3; i++) b.AddNewest(new JsonObject { ["i"] = i });
        Assert.Equal(new[] { 2, 1 }, b.NewestFirst().Select(x => (int)x["i"]!));
        Assert.Equal(1, (int)b.PopOldest()!["i"]!);
    }
}
