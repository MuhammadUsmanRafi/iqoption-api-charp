using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace BtcTickTrader;

/// <summary>Tick flags.</summary>
[Flags]
public enum TickFlags
{
    None = 0,

    /// <summary>First price after the feeder (re)connected: prices before it are from before an outage.</summary>
    Gap = 1,
}

/// <summary>One price tick as it sits in shared memory: exactly one 64-byte cache line.</summary>
[StructLayout(LayoutKind.Sequential, Size = 64)]
public struct Tick
{
    /// <summary>Tick number (1, 2, 3...). Negative while the feeder is writing the slot.</summary>
    public long Seq;

    /// <summary>Price: the MT5 bid, or the bid/ask middle with PriceFeeder --price mid.</summary>
    public double Price;

    /// <summary>Source time in Unix milliseconds; 0 when unknown (MT5 quotes carry the broker's local time, so the feeder leaves it 0).</summary>
    public long ExchangeTimeMs;

    /// <summary><see cref="Stopwatch.GetTimestamp"/> when the feeder parsed it. The clock is machine-wide, so another process can subtract it from its own.</summary>
    public long ReceivedTimestamp;

    /// <summary>Quote number since the feeder connected to MT5.</summary>
    public long SourceId;

    /// <summary>Volume MT5 sent with the quote (usually 0 for CFD quotes).</summary>
    public double Quantity;

    /// <summary>See <see cref="TickFlags"/>.</summary>
    public TickFlags Flags;
}

/// <summary>Result of <see cref="TickRing.TryRead"/>.</summary>
public enum ReadStatus
{
    /// <summary>The tick was copied out.</summary>
    Ok,

    /// <summary>The tick is not published yet.</summary>
    NotYet,

    /// <summary>The feeder already overwrote the slot; the reader fell more than <see cref="TickRing.Capacity"/> ticks behind.</summary>
    Lost,
}

/// <summary>
/// Lock-free tick ring in a memory-mapped file shared by two processes: one writer (PriceFeeder), any number of
/// readers (bots). Both map the same physical pages, so a published tick is visible to the reader as soon as the CPU
/// makes the store visible; there is no socket, pipe or copy in between. Each slot is a seqlock: the writer marks it
/// negative, writes the data, then stores the positive tick number; a reader keeps a copy only if the number was the
/// same before and after it copied.
/// </summary>
public sealed unsafe class TickRing : IDisposable
{
    /// <summary>Slots in the ring (a power of two).</summary>
    public const int Capacity = 4096;

    private const long MagicValue = 0x3247_4E49_524B_4354; // "TCKRING2" (layout 2: no Diff field)
    private const int HeaderSize = 128;
    private const long Size = HeaderSize + (long)Capacity * 64;

    // Header layout. Fields the writer changes on every tick live on their own cache line (offset 64).
    private const int MagicOffset = 0;
    private const int SessionOffset = 8;
    private const int PidOffset = 16;
    private const int SymbolOffset = 24;   // 32 bytes of ASCII
    private const int HeadOffset = 64;
    private const int LastMessageOffset = 72;

    private readonly MemoryMappedFile _file;
    private readonly MemoryMappedViewAccessor _view;
    private readonly byte* _base;
    private readonly Tick* _slots;
    private readonly bool _writer;
    private readonly FileStream? _writerLock;
    private long _head;

    private TickRing(string path, bool writer)
    {
        _writer = writer;
        if (writer)
        {
            // FileShare.None takes an OS lock (flock on macOS/Linux), held until Dispose or process exit.
            try
            {
                _writerLock = new FileStream(path + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            }
            catch (IOException)
            {
                throw new IOException($"Another PriceFeeder is already writing to {path}. Stop it first or use a different --ring.");
            }
        }
        var fs = new FileStream(path, writer ? FileMode.OpenOrCreate : FileMode.Open,
            writer ? FileAccess.ReadWrite : FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        try
        {
            if (writer) fs.SetLength(Size);
            else if (fs.Length < Size) throw new InvalidDataException($"{path} is not a tick ring (too small).");
            var access = writer ? MemoryMappedFileAccess.ReadWrite : MemoryMappedFileAccess.Read;
            _file = MemoryMappedFile.CreateFromFile(fs, null, Size, access, HandleInheritability.None, leaveOpen: false);
        }
        catch
        {
            fs.Dispose();
            _writerLock?.Dispose();
            throw;
        }
        _view = _file.CreateViewAccessor(0, Size, writer ? MemoryMappedFileAccess.ReadWrite : MemoryMappedFileAccess.Read);
        byte* p = null;
        _view.SafeMemoryMappedViewHandle.AcquirePointer(ref p);
        _base = p + _view.PointerOffset;
        _slots = (Tick*)(_base + HeaderSize);
    }

    /// <summary>Default ring file: RAM-backed /dev/shm on Linux, the user's temp folder elsewhere.</summary>
    public static string DefaultPath =>
        Path.Combine(Directory.Exists("/dev/shm") ? "/dev/shm" : Path.GetTempPath(), "btc-ticks.ring");

    /// <summary>Creates (or takes over) the ring as its only writer. Readers already attached see a new <see cref="SessionId"/> and resync.</summary>
    public static TickRing CreateWriter(string path, string symbol)
    {
        var ring = new TickRing(path, writer: true);
        Volatile.Write(ref *(long*)(ring._base + MagicOffset), 0);
        Interlocked.MemoryBarrier();
        new Span<byte>(ring._base + 8, (int)(Size - 8)).Clear();
        *(long*)(ring._base + SessionOffset) = Random.Shared.NextInt64(1, long.MaxValue);
        *(long*)(ring._base + PidOffset) = Environment.ProcessId;
        var name = System.Text.Encoding.ASCII.GetBytes(symbol.ToUpperInvariant());
        name.AsSpan(0, Math.Min(name.Length, 31)).CopyTo(new Span<byte>(ring._base + SymbolOffset, 31));
        Volatile.Write(ref *(long*)(ring._base + MagicOffset), MagicValue);
        return ring;
    }

    /// <summary>Opens an existing ring for reading, or returns null while the feeder has not created it yet.</summary>
    public static TickRing? TryOpenReader(string path)
    {
        if (!File.Exists(path)) return null;
        TickRing ring;
        try
        {
            ring = new TickRing(path, writer: false);
        }
        catch (Exception ex) when (ex is IOException or InvalidDataException or UnauthorizedAccessException)
        {
            return null;
        }
        if (Volatile.Read(ref *(long*)(ring._base + MagicOffset)) == MagicValue) return ring;
        ring.Dispose();
        return null;
    }

    /// <summary>True while the header is valid (false while a new feeder is resetting it).</summary>
    public bool IsValid => Volatile.Read(ref *(long*)(_base + MagicOffset)) == MagicValue;

    /// <summary>Random id of the feeder run that owns the ring; changes when the feeder restarts.</summary>
    public long SessionId => Volatile.Read(ref *(long*)(_base + SessionOffset));

    /// <summary>Process id of the feeder.</summary>
    public long WriterPid => Volatile.Read(ref *(long*)(_base + PidOffset));

    /// <summary>Symbol the feeder streams, e.g. BTCUSDT.</summary>
    public string Symbol => new string((sbyte*)(_base + SymbolOffset));

    /// <summary>Number of the newest published tick (0 = none yet).</summary>
    public long Head => Volatile.Read(ref *(long*)(_base + HeadOffset));

    /// <summary><see cref="Stopwatch.GetTimestamp"/> of the last quote the feeder got from MT5 (0 = never).</summary>
    public long LastMessageTimestamp => Volatile.Read(ref *(long*)(_base + LastMessageOffset));

    /// <summary>Publishes the next tick (writer only). <see cref="Tick.Seq"/> is filled in; returns its number.</summary>
    public long Publish(Tick tick)
    {
        if (!_writer) throw new InvalidOperationException("This ring was opened for reading.");
        long n = ++_head;
        Tick* slot = _slots + (n & (Capacity - 1));
        Volatile.Write(ref slot->Seq, -n);
        Interlocked.MemoryBarrier(); // the data below must not become visible before the slot is marked busy
        tick.Seq = -n;
        *slot = tick;
        Volatile.Write(ref slot->Seq, n);
        Volatile.Write(ref *(long*)(_base + HeadOffset), n);
        return n;
    }

    /// <summary>Records that a message (with or without a new price) just arrived (writer only).</summary>
    public void MarkMessage(long timestamp) => Volatile.Write(ref *(long*)(_base + LastMessageOffset), timestamp);

    /// <summary>Copies tick number <paramref name="n"/> out of the ring.</summary>
    public ReadStatus TryRead(long n, out Tick tick)
    {
        Tick* slot = _slots + (n & (Capacity - 1));
        long before = Volatile.Read(ref slot->Seq);
        if (before != n)
        {
            tick = default;
            return Math.Abs(before) > n ? ReadStatus.Lost : ReadStatus.NotYet;
        }
        tick = *slot;
        Interlocked.MemoryBarrier(); // the copy above must finish before the check below
        if (Volatile.Read(ref slot->Seq) != n) return ReadStatus.Lost;
        tick.Seq = n;
        return ReadStatus.Ok;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _view.SafeMemoryMappedViewHandle.ReleasePointer();
        _view.Dispose();
        _file.Dispose();
        _writerLock?.Dispose();
    }
}
