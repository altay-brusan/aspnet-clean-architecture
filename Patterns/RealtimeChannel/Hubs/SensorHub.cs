using Microsoft.AspNetCore.SignalR;
using RealtimeChannel.Models;

namespace RealtimeChannel.Hubs;

/// <summary>
/// SignalR hub — deliberately thin. All broadcasting is done by SensorConsumerService
/// via IHubContext, avoiding hub-method routing overhead on the hot path.
///
/// Client → Server methods:
///   SubscribeToDevice(deviceId)    — filter feed to one device
///   UnsubscribeFromDevice(deviceId)— return to all-sensors feed
///
/// Server → Client methods (sent by SensorConsumerService):
///   ReceiveSensorReadingBatch(List&lt;SensorReading&gt;)  — batched readings
///   ReceiveAlertBatch(List&lt;SensorReading&gt;)          — alert-only batch
///   ReceiveChannelStats(ChannelStats)               — pipeline health (1 s interval)
/// </summary>
public sealed class SensorHub : Hub
{
    private readonly ILogger<SensorHub> _logger;
    public SensorHub(ILogger<SensorHub> logger) => _logger = logger;

    public override async Task OnConnectedAsync()
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, HubGroups.AllSensors);
        _logger.LogInformation("Client connected: {Id}", Context.ConnectionId);
        await base.OnConnectedAsync();
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.LogInformation("Client disconnected: {Id}", Context.ConnectionId);
        return base.OnDisconnectedAsync(exception);
    }

    public async Task SubscribeToDevice(string deviceId)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, HubGroups.AllSensors);
        await Groups.AddToGroupAsync(Context.ConnectionId, HubGroups.ForDevice(deviceId));
    }

    public async Task UnsubscribeFromDevice(string deviceId)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, HubGroups.ForDevice(deviceId));
        await Groups.AddToGroupAsync(Context.ConnectionId, HubGroups.AllSensors);
    }
}

/// <summary>Centralises group name constants — no magic strings scattered in code.</summary>
public static class HubGroups
{
    public const string AllSensors = "all-sensors";
    public static string ForDevice(string deviceId) => $"device-{deviceId.ToLowerInvariant()}";
}

/// <summary>Pipeline health snapshot broadcast every second to all clients.</summary>
public sealed record ChannelStats(
    int    BufferedCount,
    int    Capacity,
    long   TotalConsumed,
    double ThroughputPerSecond,
    long   DroppedCount);
