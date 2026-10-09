using System.Globalization;
using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

/// <summary>Maps a golden case to a C# call and turns the C# result into the Python result's JSON shape.</summary>
internal static class Calls
{
    public static async Task<JsonNode?> InvokeAsync(IqOption api, FakeSocket socket, string method, JsonObject a)
    {
        switch (method)
        {
            case "Heartbeat":
            {
                socket.Push("{\"name\":\"heartbeat\",\"msg\":" + L(a, "heartbeatTime").ToString(CultureInfo.InvariantCulture) + "}");
                await api.Conn.WaitUntilAsync(() => { lock (socket.Frames) return socket.Frames.Count > 0; }, TimeSpan.FromSeconds(5), default);
                return null;
            }
            case "GetProfileAsync": return await api.GetProfileAsync();
            case "GetBalancesAsync": return await api.GetBalancesAsync();
            case "GetBalanceAsync": return ToNode(await api.GetBalanceAsync());
            case "GetBalanceId": return ToNode(api.GetBalanceId());
            case "GetBalanceModeAsync":
                return (await api.GetBalanceModeAsync()) switch { BalanceMode.Real => "REAL", BalanceMode.Practice => "PRACTICE", _ => null };
            case "GetBalanceIdByModeAsync": return ToNode(await api.GetBalanceIdByModeAsync(Mode(a)));
            case "ChangeBalanceAsync": await api.ChangeBalanceAsync(Mode(a)); return ToNode(api.GetBalanceId());
            case "GetCurrencyAsync": return await api.GetCurrencyAsync();
            case "ResetPracticeBalanceAsync": return await api.ResetPracticeBalanceAsync();
            case "GetTransactionsAsync":
                return await api.GetTransactionsAsync(L(a, "from"), L(a, "to"), I(a, "limit"), I(a, "offset"), Strings(a, "types"));

            case "GetAllInitAsync": return await api.GetAllInitAsync();
            case "GetAllProfitAsync": return Nested(await api.GetAllProfitAsync());
            case "GetAllProfitBlitzAsync": return Nested(await api.GetAllProfitBlitzAsync());
            case "UpdateActivesAsync": await api.UpdateActivesAsync(); return Items(api.GetAllActives());
            case "GetBinaryOptionDetailAsync":
                return new JsonObject((await api.GetBinaryOptionDetailAsync()).Select(x => KeyValuePair.Create(x.Key,
                    (JsonNode?)new JsonObject(x.Value.Select(y => KeyValuePair.Create(y.Key, (JsonNode?)y.Value.DeepClone()))))));
            case "GetDigitalUnderlyingListAsync": return await api.GetDigitalUnderlyingListAsync();
            case "UpdateDigitalActivesAsync": await api.UpdateDigitalActivesAsync(); return Items(api.GetAllActives());
            case "GetInstrumentsAsync": return await api.GetInstrumentsAsync(S(a, "type"));
            case "UpdateMarginActivesAsync": await api.UpdateMarginActivesAsync(SN(a, "type")); return Items(api.GetAllActives());
            case "GetAllOpenTimeAsync":
                return new JsonObject((await api.GetAllOpenTimeAsync()).Select(t => KeyValuePair.Create(t.Key,
                    (JsonNode?)new JsonObject(t.Value.Select(x => KeyValuePair.Create(x.Key, (JsonNode?)new JsonObject { ["open"] = x.Value }))))));
            case "GetFinancialInformationAsync": return await api.GetFinancialInformationAsync(I(a, "activeId"));
            case "GetNameByActiveIdAsync": return await api.GetNameByActiveIdAsync(I(a, "activeId"));
            case "GetTopAssetAsync": return await api.GetTopAssetAsync(S(a, "type"));
            case "OpcodeToName": return api.OpcodeToName(I(a, "activeId"));

            case "GetCandlesAsync":
                return new JsonArray((await api.GetCandlesAsync(S(a, "asset"), I(a, "interval"), I(a, "count"), D(a, "endTime")))
                    .Select(c => (JsonNode?)c.Raw.DeepClone()).ToArray());
            case "StartCandlesStreamAsync":
                await api.StartCandlesStreamAsync(S(a, "asset"), I(a, "size"), I(a, "maxDict"));
                return CandleMap(api.GetRealtimeCandles(S(a, "asset"), I(a, "size")));
            case "StartAllCandlesStreamsAsync":
                await api.StartAllCandlesStreamsAsync(S(a, "asset"), I(a, "maxDict"));
                return new JsonObject(api.GetRealtimeCandles(S(a, "asset"))
                    .Select(x => KeyValuePair.Create(x.Key.ToString(CultureInfo.InvariantCulture), (JsonNode?)CandleMap(x.Value))));
            case "StopCandlesStreamAsync": await api.StopCandlesStreamAsync(S(a, "asset"), I(a, "size")); return null;
            case "StopAllCandlesStreamsAsync": await api.StopAllCandlesStreamsAsync(S(a, "asset")); return null;

            case "BuyAsync":
            {
                var (ok, id, err) = await api.BuyAsync(D(a, "amount"), S(a, "asset"), Dir(a), I(a, "minutes"));
                return Tuple(ok, ok ? (object)id : err);
            }
            case "BuyMultiAsync":
                return new JsonArray((await api.BuyMultiAsync(
                        a["amounts"]!.AsArray().Select(x => J.Dbl(x)).ToList(),
                        Strings(a, "assets"),
                        Strings(a, "directions").Select(d => d == "call" ? Direction.Call : Direction.Put).ToList(),
                        a["minutes"]!.AsArray().Select(x => (int)J.Long(x)).ToList()))
                    .Select(x => ToNode(x)).ToArray());
            case "BuyByRawExpirationAsync":
            {
                var (ok, res, err) = await api.BuyByRawExpirationAsync(D(a, "amount"), S(a, "asset"), Dir(a), S(a, "optionType"), L(a, "expired"));
                return Tuple(ok, ok ? (object?)res : err);
            }
            case "BuyBlitzAsync":
            {
                var (ok, res, err) = await api.BuyBlitzAsync(D(a, "amount"), S(a, "asset"), Dir(a), I(a, "seconds"));
                return Tuple(ok, ok ? (object?)res : err);
            }
            case "SellOptionAsync": await api.SellOptionAsync(Longs(a, "ids")); return null;
            case "CheckWinAsync": return (await api.CheckWinAsync(L(a, "id"))).Profit;
            case "GetOptionInfoAsync": return await api.GetOptionInfoAsync(I(a, "limit"));
            case "GetRemainingTime": return api.GetRemainingTime(I(a, "minutes"));
            case "GetOptionOpenByOtherPc":
                return new JsonObject(api.GetOptionOpenByOtherPc().Select(x => KeyValuePair.Create(x.Key.ToString(CultureInfo.InvariantCulture), (JsonNode?)x.Value.DeepClone())));

            case "BuyDigitalSpotAsync":
            {
                var (ok, id, err) = await api.BuyDigitalSpotAsync(S(a, "asset"), D(a, "amount"), Dir(a), I(a, "minutes"));
                return Tuple(ok, ok ? (object)id : err);
            }
            case "BuyDigitalAsync":
            {
                var (ok, changed, err) = await api.BuyDigitalAsync(D(a, "amount"), S(a, "instrumentId"));
                return Tuple(ok, ok ? (object?)changed : err);
            }
            case "CloseDigitalOptionAsync": return await api.CloseDigitalOptionAsync(L(a, "id"));
            case "CloseDigitalOptionBatchAsync": return await api.CloseDigitalOptionBatchAsync(Longs(a, "ids"));
            case "CheckWinDigitalAsync": return Tuple(true, (await api.CheckWinDigitalAsync(L(a, "id"))).Profit);
            case "GetStrikeListAsync":
            {
                var (raw, strikes) = await api.GetStrikeListAsync(S(a, "asset"), I(a, "minutes"));
                return Tuple(raw, strikes == null ? null : new JsonObject(strikes.Select(x => KeyValuePair.Create(x.Key,
                    (JsonNode?)new JsonObject { ["call"] = x.Value.Call, ["put"] = x.Value.Put }))));
            }
            case "SubscribeStrikeListAsync": await api.SubscribeStrikeListAsync(S(a, "asset"), I(a, "minutes")); return null;
            case "UnsubscribeStrikeListAsync": await api.UnsubscribeStrikeListAsync(S(a, "asset"), I(a, "minutes")); return null;
            case "GetRealtimeStrikeListAsync":
                return new JsonObject((await api.GetRealtimeStrikeListAsync(S(a, "asset"), I(a, "minutes"))).Select(x => KeyValuePair.Create(x.Key,
                    (JsonNode?)new JsonObject
                    {
                        ["call"] = new JsonObject { ["id"] = x.Value.CallId, ["profit"] = x.Value.CallProfit },
                        ["put"] = new JsonObject { ["id"] = x.Value.PutId, ["profit"] = x.Value.PutProfit },
                    })));
            case "GetInstrumentQuotesGeneratedDataAsync": return await api.GetInstrumentQuotesGeneratedDataAsync(S(a, "asset"), I(a, "minutes"));
            case "GetDigitalCurrentProfit": return ToNode(api.GetDigitalCurrentProfit(S(a, "asset"), I(a, "minutes")));
            case "GetDigitalSpotProfitAfterSaleAsync": return ToNode(await api.GetDigitalSpotProfitAfterSaleAsync(L(a, "id")));
            case "GetDigitalPositionByPositionIdAsync": return await api.GetDigitalPositionByPositionIdAsync(L(a, "id"));
            case "GetDigitalPositionAsync": return await api.GetDigitalPositionAsync(L(a, "id"));
            case "GetDigitalOpenByOtherPc":
                return new JsonObject(api.GetDigitalOpenByOtherPc().Select(x => KeyValuePair.Create(x.Key.ToString(CultureInfo.InvariantCulture), (JsonNode?)x.Value.DeepClone())));

            // @@CALLS@@
            default:
                throw new ArgumentException("No C# adapter for " + method);
        }
    }

    public static void Setup(IqOption api, string? setup)
    {
        if (setup == null) return;
        var parts = setup.Split(':');
        var s = api.StateForTests;
        lock (s.Lock)
        {
            switch (parts[0])
            {
                case "candle_check": State.GetOrAdd(s.CandleCheck, parts[1])[int.Parse(parts[2], CultureInfo.InvariantCulture)] = true; break;
                case "all_size_check": s.AllSizeCheck[parts[1]] = true; break;
                default: throw new ArgumentException("unknown setup " + setup);
            }
        }
    }

    internal static JsonArray Tuple(params object?[] items) => new(items.Select(ToNode).ToArray());

    internal static JsonNode? ToNode(object? o) => o switch
    {
        null => null,
        JsonNode n => n.DeepClone(),
        bool b => b,
        string s => s,
        int i => i,
        long l => l,
        double d => d,
        _ => throw new ArgumentException("cannot convert " + o.GetType().Name),
    };

    internal static string S(JsonObject a, string k) => (string)a[k]!;

    internal static int I(JsonObject a, string k) => (int)J.Long(a[k]);

    internal static long L(JsonObject a, string k) => J.Long(a[k]);

    internal static double D(JsonObject a, string k) => J.Dbl(a[k]);

    internal static bool B(JsonObject a, string k) => J.IsTrue(a[k]);

    internal static string? SN(JsonObject a, string k) => J.IsNull(a[k]) ? null : (string)a[k]!;

    internal static double? DN(JsonObject a, string k) => J.DblOrNull(a[k]);

    internal static Direction Dir(JsonObject a) => S(a, "direction") == "call" ? Direction.Call : Direction.Put;

    internal static BalanceMode Mode(JsonObject a) => S(a, "mode") == "REAL" ? BalanceMode.Real : BalanceMode.Practice;

    internal static List<string> Strings(JsonObject a, string k) => a[k]!.AsArray().Select(x => (string)x!).ToList();

    internal static List<long> Longs(JsonObject a, string k) => a[k]!.AsArray().Select(x => J.Long(x)).ToList();

    internal static JsonObject Nested(Dictionary<string, Dictionary<string, double>> d) =>
        new(d.Select(x => KeyValuePair.Create(x.Key, (JsonNode?)new JsonObject(x.Value.Select(y => KeyValuePair.Create(y.Key, (JsonNode?)y.Value))))));

    internal static JsonObject CandleMap(IReadOnlyDictionary<long, Candle> d) =>
        new(d.Select(x => KeyValuePair.Create(x.Key.ToString(CultureInfo.InvariantCulture), (JsonNode?)x.Value.Raw.DeepClone())));

    internal static JsonArray Items(IEnumerable<KeyValuePair<string, int>> items) =>
        new(items.Select(p => (JsonNode?)new JsonArray(p.Key, p.Value)).ToArray());
}
