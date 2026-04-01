using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using RealtimeChannel.Channels;
using RealtimeChannel.Hubs;
using RealtimeChannel.Models;

namespace RealtimeChannel.Services;

/// <summary>
/// BackgroundService that drains the BoundedChannel and broadcasts readings
/// to SignalR clients using the time-bounded FlushLoop batching pattern.
///
/// ════════════════════════════════════════════════════════════════
/// THE TIME-BOUNDED FLUSHLOOP PATTERN
/// ════════════════════════════════════════════════════════════════
///
/// Each iteration creates a linked CancellationTokenSource that auto-cancels
/// after FlushIntervalMs (default 50 ms). Inside the loop, ReadAsync awaits
/// each item — collecting up to BatchSize items. When either condition is met:
///
///   1. The batch reaches BatchSize (default 1 000)  → flush immediately
///   2. The FlushIntervalMs timer expires             → flush what we have
///
/// This ensures:
///   • Under HIGH load  → large batches (up to BatchSize), fewer SendAsync calls
///   • Under LOW  load  → items wait up to FlushIntervalMs before flushing,
///     producing larger batches than a naive drain-immediately approach
///   • Latency is bounded — no item waits longer than FlushIntervalMs
///
/// On shutdown, remaining items are drained with TryRead and flushed once more.
/// ════════════════════════════════════════════════════════════════
/// </summary>
public sealed class SensorConsumerService : BackgroundService
{
    private readonly SensorDataChannel _channel;
    private readonly IHubContext<SensorHub> _hubContext;
    private readonly SensorProducerService _producer;
    private readonly SensorDispatchOptions _options;
    private readonly ILogger<SensorConsumerService> _logger;

    private long _totalConsumed;
    private long _lastConsumedSnapshot;
    private DateTimeOffset _lastStatsTime = DateTimeOffset.UtcNow;

    public SensorConsumerService(
        SensorDataChannel channel,
        IHubContext<SensorHub> hubContext,
        SensorProducerService producer,
        IOptions<SensorDispatchOptions> options,
        ILogger<SensorConsumerService> logger)
    {
        _channel    = channel;
        _hubContext = hubContext;
        _producer   = producer;
        _options    = options.Value;
        _logger     = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Consumer started (FlushLoop, BatchSize={Batch}, FlushInterval={Interval}ms)",
            _options.BatchSize, _options.FlushIntervalMs);

        return Task.WhenAll(
            FlushLoopAsync(stoppingToken),
            PublishStatsLoopAsync(stoppingToken));
    }

    // ── FlushLoop ────────────────────────────────────────────────────────────

    private async Task FlushLoopAsync(CancellationToken stoppingToken)
    {
        var batch = new List<SensorReading>(_options.BatchSize);
        var reader = _channel.Reader;

        while (!stoppingToken.IsCancellationRequested)
        {
            batch.Clear();

            // Create a linked token that auto-cancels after FlushIntervalMs.
            // This gives us a time-bounded window to collect items.
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            cts.CancelAfter(TimeSpan.FromMilliseconds(_options.FlushIntervalMs));

            try
            {
                while (batch.Count < _options.BatchSize)
                {
                    // ReadAsync WAITS for the next item — unlike TryRead, this lets us
                    // accumulate items over the full FlushInterval window rather than
                    // flushing immediately when the buffer is momentarily empty.
                    var item = await reader.ReadAsync(cts.Token);
                    batch.Add(item);
                }
            }
            catch (OperationCanceledException)
            {
                // Either the flush interval expired or the service is stopping.
                // In both cases, flush whatever we have collected so far.
            }

            if (batch.Count > 0)
            {
                await FlushBatchAsync(batch, stoppingToken);

                Interlocked.Add(ref _totalConsumed, batch.Count);
                _logger.LogTrace("Flushed {Count} readings | buffered={Buf}",
                    batch.Count, _channel.ApproximateCount);
            }
        }

        // Drain remaining items after cancellation
        while (reader.TryRead(out var remaining))
            batch.Add(remaining);

        if (batch.Count > 0)
            await FlushBatchAsync(batch, CancellationToken.None);

        _logger.LogInformation("FlushLoop stopped. Total consumed: {Total}", _totalConsumed);
    }

    private async Task FlushBatchAsync(List<SensorReading> batch, CancellationToken ct)
    {
        // 1 — all-sensors group: one SendAsync for the whole batch
        await _hubContext.Clients
            .Group(HubGroups.AllSensors)
            .SendAsync("ReceiveSensorReadingBatch", batch, ct);

        // 2 — per-device groups: one SendAsync per device sub-batch
        foreach (var grp in batch.GroupBy(r => r.DeviceId))
        {
            await _hubContext.Clients
                .Group(HubGroups.ForDevice(grp.Key))
                .SendAsync("ReceiveSensorReadingBatch", grp.ToList(), ct);
        }

        // 3 — alerts group: only allocates when alerts exist (~3 % of readings)
        var alerts = batch.Where(r => r.IsAlert).ToList();
        if (alerts.Count > 0)
        {
            await _hubContext.Clients
                .Group("alerts")
                .SendAsync("ReceiveAlertBatch", alerts, ct);

            foreach (var a in alerts)
                _logger.LogWarning("ALERT: {Reading}", a);
        }
    }

    // ── Stats ticker ─────────────────────────────────────────────────────────

    private async Task PublishStatsLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                var now     = DateTimeOffset.UtcNow;
                var elapsed = (now - _lastStatsTime).TotalSeconds;
                var consumed = Interlocked.Read(ref _totalConsumed);
                var throughput = elapsed > 0 ? (consumed - _lastConsumedSnapshot) / elapsed : 0;
                _lastConsumedSnapshot = consumed;
                _lastStatsTime        = now;

                var stats = new ChannelStats(
                    _channel.ApproximateCount,
                    _channel.Capacity,
                    consumed,
                    Math.Round(throughput, 1),
                    Interlocked.Read(ref _producer._droppedCount));

                await _hubContext.Clients.All.SendAsync("ReceiveChannelStats", stats, ct);
            }
        }
        catch (OperationCanceledException) { /* graceful shutdown */ }
    }
}
