using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

public class PyJsonTests
{
    [Fact]
    public void Frame_UsesPythonSeparatorsAndKeyOrder() =>
        Assert.Equal("{\"name\": \"sendMessage\", \"msg\": {\"name\": \"get-balances\", \"version\": \"1.0\"}, \"request_id\": \"\"}",
            PyJson.Frame("sendMessage", new Obj { ["name"] = "get-balances", ["version"] = "1.0" }, ""));

    [Theory]
    [InlineData(1.0, "1.0")]
    [InlineData(0.1, "0.1")]
    [InlineData(1e-05, "1e-05")]
    [InlineData(1e16, "1e+16")]
    [InlineData(1e15, "1000000000000000.0")]
    [InlineData(123.456, "123.456")]
    [InlineData(-0.001, "-0.001")]
    [InlineData(1700000747.0, "1700000747.0")]
    [InlineData(2.5e-300, "2.5e-300")]
    public void FloatRepr_MatchesPythonRepr(double value, string expected) =>
        Assert.Equal(expected, PyJson.FloatRepr(value));

    [Theory]
    [InlineData(1.0, "1")]
    [InlineData(1.5, "1.5")]
    [InlineData(10.0, "10")]
    public void RawNumber_IntegralPrintsLikePythonInt(double value, string expected) =>
        Assert.Equal(expected, PyJson.Dumps(value));

    [Fact]
    public void PyFloat_AlwaysHasFraction() => Assert.Equal("1.0", PyJson.Dumps(new PyFloat(1)));

    [Fact]
    public void Strings_AreAsciiEscapedLowercase() =>
        Assert.Equal("\"a\\\"b\\\\c\\n\\u00e9\\u007f\\u20ac\"", PyJson.Dumps("a\"b\\c\né\u007f€"));

    [Fact]
    public void Lists_NullsBools_AndEmptyKey() =>
        Assert.Equal("{\"a\": [1, \"x\", null, true, false], \"\": 2, \"b\": {}}",
            PyJson.Dumps(new Obj { ["a"] = new object?[] { 1, "x", null, true, false }, [""] = 2, ["b"] = new Obj() }));

    [Fact]
    public void JsonNode_KeepsIntAndFloatForms() =>
        Assert.Equal("{\"i\": 5, \"f\": 2.0, \"s\": \"t\"}", PyJson.Dumps(JsonNode.Parse("{\"i\":5,\"f\":2.0,\"s\":\"t\"}")));

    [Fact]
    public void J_ReadsNumbersStringsAndTruncatesLikePythonInt()
    {
        var o = JsonNode.Parse("{\"a\":\"2.5\",\"b\":7,\"c\":3.9,\"d\":true,\"e\":null}")!.AsObject();
        Assert.Equal(2.5, J.Dbl(o["a"]));
        Assert.Equal(7, J.Long(o["b"]));
        Assert.Equal(3, J.Long(o["c"]));
        Assert.True(J.IsTrue(o["d"]));
        Assert.Null(J.DblOrNull(o["e"]));
        Assert.Equal("7", J.Str(o["b"]));
    }
}
