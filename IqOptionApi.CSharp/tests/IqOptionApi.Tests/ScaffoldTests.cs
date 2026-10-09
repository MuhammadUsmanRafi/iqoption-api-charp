namespace IqOptionApi.Tests;

public class ScaffoldTests
{
    [Fact]
    public void LibraryAssemblyLoads() =>
        Assert.Equal("IqOptionApi", typeof(IqOptionApi.Candle).Assembly.GetName().Name);
}
