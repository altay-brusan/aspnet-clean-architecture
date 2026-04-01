using System.Threading.Channels;
using Microsoft.Extensions.Options;
using RealtimeChannel.Models;

namespace RealtimeChannel.Channels;

/// <summary>
/// Singleton wrapper around a <see cref="BoundedChannel{T}"/> of <see cref="SensorReading"/>.
///
/// WHY BOUNDED?
///   An unbounded channel accepts writes forever — memory grows without limit if the consumer
///   falls behind. BoundedChannel enforces a hard capacity and applies a FullMode strategy:
///
///   • Wait       — producer awaits until space is free (backpressure)  ← used here
///   • DropOldest — oldest buffered item is removed to make room
///   • DropNewest — incoming item is discarded
///
///   Wait applies backpressure: the producer blocks when the buffer is full,
///   preventing data loss and signalling the system to slow down.
/// </summary>
public sealed class SensorDataChannel
{
    private readonly Channel<SensorReading> _channel;

    public int Capacity { get; }

    public SensorDataChannel(IOptions<SensorDispatchOptions> options)
    {
        var opts = options.Value;
        Capacity = opts.ChannelCapacity;

        _channel = Channel.CreateBounded<SensorReading>(new BoundedChannelOptions(Capacity)
        {
            SingleWriter = opts.SingleWriter,
            SingleReader = opts.SingleReader,
            FullMode     = BoundedChannelFullMode.Wait,
            AllowSynchronousContinuations = false
        });
    }

    public ChannelReader<SensorReading> Reader => _channel.Reader;
    public ChannelWriter<SensorReading> Writer => _channel.Writer;

    /// <summary>Signals the channel is complete so consumers can drain and exit.</summary>
    public void Complete(Exception? error = null) => _channel.Writer.Complete(error);

    /// <summary>Advisory buffer depth for monitoring (may be stale in multi-thread context).</summary>
    public int ApproximateCount => _channel.Reader.Count;
}
