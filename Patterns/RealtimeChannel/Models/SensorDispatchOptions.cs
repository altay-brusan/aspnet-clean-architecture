namespace RealtimeChannel.Models;

/// <summary>
/// Configuration for the BoundedChannel + FlushLoop pipeline.
/// Bind from appsettings.json section "SensorDispatch".
/// </summary>
public sealed class SensorDispatchOptions
{
    public int ChannelCapacity { get; set; } = 10_000;
    public int BatchSize { get; set; } = 1_000;
    public int FlushIntervalMs { get; set; } = 50;
    public bool SingleReader { get; set; } = true;
    public bool SingleWriter { get; set; } = false;
}
