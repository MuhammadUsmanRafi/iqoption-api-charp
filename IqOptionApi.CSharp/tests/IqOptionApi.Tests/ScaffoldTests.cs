namespace IqOptionApi.Tests;

public class ScaffoldTests
{
    [Fact]
    public void LibraryAssemblyLoads() =>
        Assert.Equal("IqOptionApi", typeof(IqOptionApi.Marker).Assembly.GetName().Name);
}
