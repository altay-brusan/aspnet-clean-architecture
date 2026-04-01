using RealtimeChannel.Channels;
using RealtimeChannel.Models;

namespace RealtimeChannel.Services;

/// <summary>
/// BackgroundService that simulates IoT sensor devices and writes
/// SensorReading objects into the BoundedChannel at high frequency.
///
/// Uses PeriodicTimer (not Task.Delay) to avoid drift accumulation on tight loops.
/// Uses TryWrite (non-blocking) because DropOldest always makes room.
/// Calls channel.Complete() on shutdown so the consumer drains cleanly.
/// </summary>
public sealed class SensorProducerService : BackgroundService
{
    private const int DeviceCount = 10;
    private static readonly TimeSpan ProducerInterval = TimeSpan.FromMilliseconds(50);

    private static readonly (string Name, string Unit, double Min, double Max, double AlertAt)[] Metrics =
    [
        ("Temperature", "°C",  -10,  85,  80),
        ("Voltage",     "V",   210, 240, 245),
        ("Current",     "A",     0,  16,  15),
        ("Humidity",    "%",    20,  80,  85),
        ("Pressure",    "hPa", 950, 1050, 1060),
    ];

    private readonly SensorDataChannel _channel;
    private readonly ILogger<SensorProducerService> _logger;
    private readonly Random _rng = new();

    private long _producedCount;
    internal long _droppedCount = 0;   // always 0 with FullMode.Wait backpressure; kept for stats contract

    public SensorProducerService(SensorDataChannel channel, ILogger<SensorProducerService> logger)
    {
        _channel = channel;
        _logger  = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Producer started — {Devices} devices, {Interval}ms interval",
            DeviceCount, ProducerInterval.TotalMilliseconds);

        using var timer = new PeriodicTimer(ProducerInterval);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
                await ProduceTickAsync(stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _channel.Complete();
            _logger.LogInformation(
                "Producer stopped. Written: {P}", _producedCount);
        }
    }

    private async Task ProduceTickAsync(CancellationToken stoppingToken)
    {
        for (var device = 1; device <= DeviceCount; device++)
        {
            var metricIndex = (int)((_producedCount / DeviceCount) % Metrics.Length);
            var (name, unit, min, max, alertAt) = Metrics[metricIndex];
            var reading = BuildReading($"SENSOR-{device:D3}", name, unit, min, max, alertAt);

            // WriteAsync applies backpressure — if the channel is full,
            // the producer awaits until the consumer frees up space.
            await _channel.Writer.WriteAsync(reading, stoppingToken);
            Interlocked.Increment(ref _producedCount);
        }
    }

    private SensorReading BuildReading(string deviceId, string metric, string unit,
                                        double min, double max, double alertAt)
    {
        var value = min + _rng.NextDouble() * (max - min);
        value    += ((_rng.NextDouble() - 0.5) * 2) * (max - min) * 0.05;  // ±5 % noise

        // 3 % anomaly spike above alert threshold
        if (_rng.NextDouble() < 0.03)
            value = alertAt + _rng.NextDouble() * (max - min) * 0.2;

        // 5 % quality degradation
        var quality = _rng.NextDouble() < 0.05
            ? 0.2 + _rng.NextDouble() * 0.3
            : 0.85 + _rng.NextDouble() * 0.15;

        return new SensorReading
        {
            DeviceId   = deviceId,
            MetricType = metric,
            Value      = Math.Round(value, 2),
            Unit       = unit,
            IsAlert    = value >= alertAt,
            Quality    = Math.Round(quality, 3),
        };
    }
}
