using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

internal static class Golden
{
    public static JsonObject Load(string file) =>
        JsonNode.Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Golden", file)))!.AsObject();

    public static TimeZoneInfo Zone(string ianaId) => TimeZoneInfo.FindSystemTimeZoneById(ianaId);
}
