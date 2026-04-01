namespace RealtimeChannel.Models;

/// <summary>
/// Represents a single telemetry reading from an IoT sensor device.
/// Immutable record — safe to pass across thread boundaries through the channel.
/// </summary>
public sealed record SensorReading
{
    public required string DeviceId   { get; init; }
    public required string MetricType { get; init; }
    public required double Value      { get; init; }
    public required string Unit       { get; init; }
    public DateTimeOffset  Timestamp  { get; init; } = DateTimeOffset.UtcNow;
    public bool            IsAlert    { get; init; }
    public double          Quality    { get; init; } = 1.0;

    public override string ToString() =>
        $"[{Timestamp:HH:mm:ss.fff}] {DeviceId} | {MetricType}: {Value:F2} {Unit}" +
        (IsAlert ? " ⚠ ALERT" : string.Empty);
}
