namespace IqOptionApi.Tests;

public class TablesTests
{
    [Fact]
    public void ActivesMatchPythonInOrder()
    {
        var expected = Golden.Load("tables.json")["actives"]!.AsArray();
        var actual = new ActivesTable().Items();
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < actual.Count; i++)
        {
            Assert.Equal((string)expected[i]![0]!, actual[i].Key);
            Assert.Equal((int)expected[i]![1]!, actual[i].Value);
        }
    }

    [Fact]
    public void CountriesMatchPython()
    {
        foreach (var row in Golden.Load("tables.json")["countries"]!.AsArray())
            Assert.Equal((int)row![1]!, Countries.Ids[(string)row[0]!]);
    }

    [Fact]
    public void Get_UnknownName_Throws()
    {
        var ex = Assert.Throws<KeyNotFoundException>(() => new ActivesTable().Get("EURUSDD"));
        Assert.Contains("EURUSDD", ex.Message);
    }

    [Fact]
    public void Set_ExistingKeepsPosition_AndReverseLookupReturnsFirstName()
    {
        var t = new ActivesTable();
        int index = t.Items().ToList().FindIndex(p => p.Key == "EURUSD");
        t.Set("EURUSD", 76);
        Assert.Equal(index, t.Items().ToList().FindIndex(p => p.Key == "EURUSD"));
        Assert.Equal(t.Items().First(p => p.Value == 76).Key, t.NameOf(76));
        t.Set("NEWASSET", 9999);
        Assert.Equal("NEWASSET", t.Items()[^1].Key);
    }

    [Fact]
    public void DigitalSpotInstrumentId_UsesUtcAndNewFormat() =>
        Assert.Equal("do1A20210224D154900T1MCSPT", Helpers.DigitalSpotInstrumentId(1, 1614181740, 1, Direction.Call));

    [Fact]
    public void ResultOf_UsesSign()
    {
        Assert.Equal(TradeResult.Win, Helpers.ResultOf(0.8));
        Assert.Equal(TradeResult.Loss, Helpers.ResultOf(-1));
        Assert.Equal(TradeResult.Equal, Helpers.ResultOf(0));
    }
}
