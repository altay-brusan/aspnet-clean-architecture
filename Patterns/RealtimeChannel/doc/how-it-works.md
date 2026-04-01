# How the Project Works

This is a real-time IoT sensor dashboard built with **ASP.NET Core**, **System.Threading.Channels**, and **SignalR**. It demonstrates high-throughput data processing using the **producer-consumer pattern** with time-bounded batching.

## The Pipeline

```
SensorProducerService          BoundedChannel (10,000)         SensorConsumerService
  (generates fake IoT data)  ──── WriteAsync ────►  buffer  ──── ReadAsync ────►  (batches & broadcasts)
  10 devices × 20 ticks/sec      backpressure if full              │
  = 200 readings/sec                                               ▼
                                                              SignalR Hub
                                                                   │
                                                              WebSocket
                                                                   │
                                                              Browser Dashboard
```

## Step by Step

### 1. Producer (`Services/SensorProducerService.cs`)

Every 50ms, it generates 10 simulated sensor readings (temperature, voltage, current, humidity, pressure) and writes them into the channel with `WriteAsync`. If the channel is full (10,000 items), `WriteAsync` blocks — this is **backpressure**, preventing uncontrolled memory growth.

### 2. BoundedChannel (`Channels/SensorDataChannel.cs`)

A thread-safe, in-memory buffer sitting between producer and consumer. It's configured with `FullMode.Wait` so the producer slows down when the consumer can't keep up. `SingleReader = true` enables internal lock-free optimizations since only one consumer reads.

### 3. Consumer / FlushLoop (`Services/SensorConsumerService.cs`)

This is the core of the design. Two concurrent tasks run:

#### FlushLoopAsync — The time-bounded batching loop

```
Each iteration:
  1. Start a 50ms timer (CancelAfter)
  2. Call ReadAsync in a loop, collecting items into a batch
  3. Stop when EITHER:
     - Batch reaches 1,000 items (BatchSize), OR
     - The 50ms timer expires (OperationCanceledException)
  4. Send the batch to SignalR clients in ONE call
  5. Repeat
```

This is the key insight from [the article](https://thecodeman.net/posts/high-throughput-real-time-data-bounded-channel-signalr). Under light load, items accumulate for up to 50ms before flushing — producing fewer, larger batches. Under heavy load, batches fill to 1,000 quickly. Either way, you get far fewer `SendAsync` calls than a naive one-item-at-a-time approach.

#### PublishStatsLoopAsync

Every 1 second, broadcasts pipeline health (buffer depth, throughput, dropped count) to all clients.

### 4. SignalR Hub (`Hubs/SensorHub.cs`)

Thin hub. Clients auto-join the `all-sensors` group on connect. They can also subscribe to a specific device group. The hub itself doesn't do broadcasting — `SensorConsumerService` uses `IHubContext<SensorHub>` to push data directly, which is more efficient.

### 5. Broadcasting (`FlushBatchAsync`)

Each batch gets sent to three audiences:

- **`all-sensors` group** — every connected client
- **Per-device groups** — clients filtering by a specific sensor
- **`alerts` group** — only readings that exceed alert thresholds (~3%)

### 6. Browser Dashboard (`wwwroot/index.html`)

Vanilla JS + SignalR client that receives batches and renders:

- Live sensor feed with batch boundaries
- Per-device sidebar with alert indicators
- Sparkline charts for throughput and batch size
- Real-time stats (throughput, buffer load, total consumed)

## Why This Architecture Matters

| Naive approach | FlushLoop approach |
|---|---|
| 200 `SendAsync`/sec (one per reading) | ~20 `SendAsync`/sec (batched) |
| 200 serializations, 200 WebSocket frames | ~20 serializations, ~20 frames |
| High async overhead per item | Amortized across batch |
| Latency: immediate but wasteful | Latency: bounded at 50ms max |

## Configuration

All tuning knobs are in `appsettings.json` under the `SensorDispatch` section:

| Setting | Default | Purpose |
|---|---|---|
| `ChannelCapacity` | 10,000 | Max items buffered before backpressure kicks in |
| `BatchSize` | 1,000 | Max items per flush batch |
| `FlushIntervalMs` | 50 | Max time (ms) to wait before flushing a partial batch |
| `SingleReader` | true | Enables lock-free channel optimizations |
| `SingleWriter` | false | Set true if only one producer thread writes |
