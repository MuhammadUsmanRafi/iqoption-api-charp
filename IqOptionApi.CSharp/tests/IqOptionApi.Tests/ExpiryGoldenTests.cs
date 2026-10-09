using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

public class ExpiryGoldenTests
{
    public static IEnumerable<object[]> Zones() =>
        new[] { "UTC", "Asia/Karachi", "Asia/Kolkata", "Asia/Kathmandu", "Australia/Adelaide", "America/New_York" }
            .Select(z => new object[] { z });

    [Theory, MemberData(nameof(Zones))]
    public void MatchesPython(string zone)
    {
        var data = Golden.Load("expiry_" + zone.Replace("/", "_") + ".json");
        var tz = Golden.Zone(zone);
        foreach (var row in data["cases"]!.AsArray().Select(r => r!.AsObject()))
        {
            long ts = (long)row["ts"]!;
            foreach (var (d, expected) in row["exp"]!.AsObject())
            {
                var (exp, idx) = Helpers.GetExpirationTime(ts, int.Parse(d), ts, tz);
                Assert.True((long)expected![0]! == exp && (int)expected[1]! == idx, $"exp ts={ts} d={d}: {exp},{idx} vs {expected.ToJsonString()}");
            }
            AssertRemaining(row["remaining"]!.AsArray(), Helpers.GetRemainingTime(ts, ts, tz), ts);
            AssertRemaining(row["remaining_f"]!.AsArray(), Helpers.GetRemainingTime(ts + 0.37, ts + 0.37, tz), ts);
            foreach (var (d, expected) in row["digital"]!.AsObject())
            {
                var (value, isFloat) = Helpers.DigitalExp(ts, int.Parse(d), ts, tz);
                Assert.True((double)expected![0]! == value && (bool)expected[1]! == isFloat, $"digital ts={ts} d={d}");
            }
            foreach (var (d, expected) in row["strike"]!.AsObject())
                Assert.Equal((long)expected!, Helpers.StrikeListExpiration(ts, int.Parse(d), tz));
        }
    }

    private static void AssertRemaining(JsonArray expected, List<(int Label, long Seconds)> actual, long ts)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < actual.Count; i++)
            Assert.True((int)expected[i]![0]! == actual[i].Label && (long)expected[i]![1]! == actual[i].Seconds, $"remaining ts={ts} i={i}");
    }

    [Theory]
    [InlineData(2.675, 2.67)]
    [InlineData(0.125, 0.12)]
    [InlineData(0.375, 0.38)]
    [InlineData(-1.005, -1.0)]
    [InlineData(12.344999, 12.34)]
    public void PyRound2_MatchesPythonRound(double x, double expected) => Assert.Equal(expected, Helpers.PyRound2(x));

    [Fact]
    public void Formulas_MatchPython()
    {
        Assert.Equal(0.8, Helpers.PayoutFraction(20));
        Assert.Equal(56, Helpers.BlitzPercent(Helpers.PayoutFraction(43)));
        Assert.Equal(57, Helpers.BlitzPercent(Helpers.PayoutFraction(42)));
        Assert.Equal("1.123450", Helpers.StrikeKey(1123450));
        Assert.Equal(((100 - 52.5) * 100) / 52.5, Helpers.DigitalProfitPercent(52.5));
        Assert.True(Helpers.TryParseSpotMinutes("do1A20210224D154900T1MCSPT", out int a) && a == 1);
        Assert.True(Helpers.TryParseSpotMinutes("doEURUSD201907191250PT5MPSPT", out int b) && b == 5);
        Assert.False(Helpers.TryParseSpotMinutes("nonsense", out _));
    }
}
