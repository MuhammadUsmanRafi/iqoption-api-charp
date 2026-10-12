namespace BtcTickTrader;

/// <summary>
/// Reads KEY=VALUE lines from a <c>.env</c> file into the environment. It looks in the current folder and its parents,
/// then next to the program and its parents, and uses the first file found. A variable already set in the
/// environment wins over the file. Shared by PriceFeeder (MT5 account) and BitcoinTickBot (IQ Option ssid).
/// </summary>
internal static class DotEnv
{
    /// <summary>Keys that were set from the file.</summary>
    public static HashSet<string> FromFile { get; } = new(StringComparer.Ordinal);

    /// <summary>Loads the first <c>.env</c> found; returns its path, or null when there is none.</summary>
    public static string? Load()
    {
        string? path = Find(Directory.GetCurrentDirectory()) ?? Find(AppContext.BaseDirectory);
        if (path == null) return null;
        foreach (string raw in File.ReadAllLines(path))
        {
            string line = raw.Trim();
            if (line.Length == 0 || line[0] == '#') continue;
            if (line.StartsWith("export ", StringComparison.Ordinal)) line = line[7..].TrimStart();
            int eq = line.IndexOf('=');
            if (eq <= 0) continue;
            string key = line[..eq].Trim();
            string value = line[(eq + 1)..].Trim();
            if (value.Length >= 2 && (value[0] == '"' || value[0] == '\'') && value[^1] == value[0]) value = value[1..^1];
            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable(key))) continue;
            Environment.SetEnvironmentVariable(key, value);
            FromFile.Add(key);
        }
        return path;
    }

    private static string? Find(string start)
    {
        for (var dir = new DirectoryInfo(start); dir != null; dir = dir.Parent)
        {
            string candidate = Path.Combine(dir.FullName, ".env");
            if (File.Exists(candidate)) return candidate;
        }
        return null;
    }
}
