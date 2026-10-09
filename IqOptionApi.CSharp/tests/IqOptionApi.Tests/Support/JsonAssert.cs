using System.Text.Json;
using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

internal static class JsonAssert
{
    public static void Equal(JsonNode? expected, JsonNode? actual, string path = "$")
    {
        if (J.IsNull(expected) || J.IsNull(actual))
        {
            Assert.True(J.IsNull(expected) && J.IsNull(actual), $"{path}: expected {expected?.ToJsonString() ?? "null"} but got {actual?.ToJsonString() ?? "null"}");
            return;
        }
        switch (expected)
        {
            case JsonObject eo:
                var ao = Assert.IsType<JsonObject>(actual);
                Assert.True(eo.Select(p => p.Key).OrderBy(k => k, StringComparer.Ordinal).SequenceEqual(ao.Select(p => p.Key).OrderBy(k => k, StringComparer.Ordinal)),
                    $"{path}: keys differ. expected [{string.Join(",", eo.Select(p => p.Key))}] got [{string.Join(",", ao.Select(p => p.Key))}]");
                foreach (var (k, v) in eo) Equal(v, ao[k], path + "." + k);
                break;
            case JsonArray ea:
                var aa = Assert.IsType<JsonArray>(actual);
                Assert.True(ea.Count == aa.Count, $"{path}: length {ea.Count} vs {aa.Count}");
                for (int i = 0; i < ea.Count; i++) Equal(ea[i], aa[i], $"{path}[{i}]");
                break;
            default:
                var kind = expected!.GetValueKind();
                if (kind == JsonValueKind.Number && actual!.GetValueKind() == JsonValueKind.Number)
                {
                    double e = J.Dbl(expected), a = J.Dbl(actual);
                    Assert.True(Math.Abs(e - a) <= 1e-9 * Math.Max(1, Math.Abs(e)), $"{path}: {e} vs {a}");
                }
                else
                {
                    Assert.True(expected.ToJsonString() == actual!.ToJsonString(), $"{path}: {expected.ToJsonString()} vs {actual.ToJsonString()}");
                }
                break;
        }
    }

    public static JsonNode? Prune(JsonNode? n)
    {
        switch (n)
        {
            case JsonObject o:
                var result = new JsonObject();
                foreach (var (k, v) in o)
                {
                    var p = Prune(v);
                    if (p is JsonObject { Count: 0 } || p is JsonArray { Count: 0 }) continue;
                    result[k] = p;
                }
                return result;
            case JsonArray a:
                return new JsonArray(a.Select(Prune).ToArray());
            default:
                return n?.DeepClone();
        }
    }
}
