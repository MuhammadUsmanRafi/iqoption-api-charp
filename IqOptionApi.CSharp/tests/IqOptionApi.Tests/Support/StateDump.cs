using System.Globalization;
using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

/// <summary>Turns <see cref="State"/> into the same JSON shape as Python's dump_state().</summary>
internal static class StateDump
{
    private static string K(long v) => v.ToString(CultureInfo.InvariantCulture);

    public static JsonObject Of(State s)
    {
        lock (s.Lock)
        {
            var o = new JsonObject
            {
                ["server_timestamp"] = s.ServerTimestamp,
                ["balance_id"] = s.BalanceId,
                ["real_time_candles"] = Nest(s.Candles, buf => new JsonObject(buf.Select(c => KeyValuePair.Create(K(c.Key), (JsonNode?)c.Value.Raw.DeepClone())))),
                ["candle_generated_check"] = new JsonObject(s.CandleCheck.Select(a => KeyValuePair.Create(a.Key,
                    (JsonNode?)new JsonObject(a.Value.Where(x => x.Value).Select(x => KeyValuePair.Create(K(x.Key), (JsonNode?)true)))))),
                ["candle_generated_all_size_check"] = new JsonObject(s.AllSizeCheck.Where(x => x.Value).Select(x => KeyValuePair.Create(x.Key, (JsonNode?)true))),
                ["traders_mood"] = new JsonObject(s.TradersMood.Select(x => KeyValuePair.Create(K(x.Key), (JsonNode?)x.Value))),
                ["commission"] = new JsonObject(s.Commission.Select(t => KeyValuePair.Create(t.Key, (JsonNode?)new JsonObject(t.Value.Select(a =>
                    KeyValuePair.Create(a.Key, (JsonNode?)new JsonObject(a.Value.Select(x => KeyValuePair.Create(PyJson.FloatRepr(x.Key), (JsonNode?)x.Value))))))))),
                ["top_assets_updated"] = new JsonObject(s.TopAssetsUpdated.Select(x => KeyValuePair.Create(x.Key, x.Value?.DeepClone()))),
                ["live_deal"] = new JsonObject(s.LiveDeals.Select(x => KeyValuePair.Create($"{x.Key.Name}|{x.Key.Asset}|{x.Key.Type}",
                    (JsonNode?)new JsonArray(x.Value.NewestFirst().Select(m => (JsonNode?)m.DeepClone()).ToArray())))),
                ["order_async"] = new JsonObject(s.OrderAsync.Select(x => KeyValuePair.Create(K(x.Key),
                    (JsonNode?)new JsonObject(x.Value.Select(v => KeyValuePair.Create(v.Key, (JsonNode?)v.Value.DeepClone())))))),
                ["option_opened"] = new JsonObject(s.OptionOpened.Select(x => KeyValuePair.Create(K(x.Key), (JsonNode?)x.Value.DeepClone()))),
                ["digital_opened"] = new JsonObject(s.DigitalOpened.Select(x => KeyValuePair.Create(K(x.Key), (JsonNode?)x.Value.DeepClone()))),
                ["digital_profit"] = Nest(s.DigitalProfit, v => v is double d ? JsonValue.Create(d) : null),
                ["quotes"] = Nest(s.Quotes, q => new JsonObject(q.Select(x => KeyValuePair.Create(x.Key, (JsonNode?)new JsonObject
                {
                    ["call"] = new JsonObject { ["id"] = x.Value.CallId, ["profit"] = x.Value.CallProfit },
                    ["put"] = new JsonObject { ["id"] = x.Value.PutId, ["profit"] = x.Value.PutProfit },
                })))),
                ["quotes_raw"] = Nest(s.QuotesRaw, m => m.DeepClone()),
                ["balance"] = new JsonObject(s.Balance.Select(x => KeyValuePair.Create(K(x.Key), (JsonNode?)x.Value))),
            };
            return (JsonObject)JsonAssert.Prune(o)!;
        }
    }

    private static JsonObject Nest<T>(Dictionary<string, Dictionary<int, T>> d, Func<T, JsonNode?> leaf) =>
        new(d.Select(a => KeyValuePair.Create(a.Key, (JsonNode?)new JsonObject(a.Value.Select(x => KeyValuePair.Create(K(x.Key), leaf(x.Value)))))));
}
