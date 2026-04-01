using RealtimeChannel.Channels;
using RealtimeChannel.Hubs;
using RealtimeChannel.Models;
using RealtimeChannel.Services;

var builder = WebApplication.CreateBuilder(args);

// ── Logging ──────────────────────────────────────────────────────────────────
builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.SetMinimumLevel(LogLevel.Information);

// ── Dispatch options (from appsettings.json) ─────────────────────────────────
builder.Services.Configure<SensorDispatchOptions>(
    builder.Configuration.GetSection("SensorDispatch"));

// ── Channel (Singleton — shared between producer and consumer) ───────────────
builder.Services.AddSingleton<SensorDataChannel>();

// ── SignalR ───────────────────────────────────────────────────────────────────
builder.Services.AddSignalR(o =>
{
    o.EnableDetailedErrors      = builder.Environment.IsDevelopment();
    o.MaximumReceiveMessageSize = 64 * 1024;   // 64 KB
    o.ClientTimeoutInterval     = TimeSpan.FromSeconds(30);
    o.KeepAliveInterval         = TimeSpan.FromSeconds(10);
});

// ── Background services ───────────────────────────────────────────────────────
// Producer registered as Singleton so Consumer can inject it to read _droppedCount.
builder.Services.AddSingleton<SensorProducerService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<SensorProducerService>());
builder.Services.AddHostedService<SensorConsumerService>();

// ── Static files (wwwroot/index.html) ────────────────────────────────────────
builder.Services.AddDirectoryBrowser();

var app = builder.Build();

if (app.Environment.IsDevelopment())
    app.UseDeveloperExceptionPage();

app.UseDefaultFiles();
app.UseStaticFiles();

// ── Endpoints ─────────────────────────────────────────────────────────────────
app.MapHub<SensorHub>("/hubs/sensor");

app.MapGet("/health", (SensorDataChannel ch) => Results.Ok(new
{
    status   = "healthy",
    buffered = ch.ApproximateCount,
    capacity = ch.Capacity,
    utc      = DateTimeOffset.UtcNow,
}));

app.Run();
