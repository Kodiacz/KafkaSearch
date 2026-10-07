namespace KafkaSearch.Core.ConsumerPooling;

using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Models;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

public sealed class ClusterConsumerPool : IDisposable
{
    private readonly IKafkaClientFactory _kafkaClientFactory;
    private readonly ClusterProfile _clusterProfile;
    private readonly int _minSize;
    private readonly SemaphoreSlim _creationGate;
    private readonly ConcurrentStack<PooledConsumerEntry> _idle = new();
    private readonly Timer _sweepTimer;
    private readonly TimeSpan? _idleTimeout;
    private ILogger<ClusterConsumerPool> _logger;

    public ClusterConsumerPool(
        IKafkaClientFactory kafkaClientFactory,
        ClusterProfile clusterProfile,
        ILogger<ClusterConsumerPool> logger,
        int minSize = 1,
        int maxSize = 4,
        TimeSpan? sweepInterval = null,
        TimeSpan? idleTimeout = null)
    {
        _kafkaClientFactory = kafkaClientFactory;
        _clusterProfile = clusterProfile;
        _logger = logger;
        _minSize = minSize;
        _creationGate = new SemaphoreSlim(maxSize, maxSize);
        _idleTimeout = idleTimeout ?? TimeSpan.FromMinutes(5);

        var interval = sweepInterval ?? TimeSpan.FromMinutes(1);
        _sweepTimer = new Timer(_ => SweepIdle(), null, interval, interval);
    }

    public async Task<RentedConsumer> RentAsync(CancellationToken ct = default)
    {
        if (_idle.TryPop(out var entry))
            return new RentedConsumer(this, entry);

        await _creationGate.WaitAsync(ct);
        var consumer = _kafkaClientFactory.CreateConsumer(
            _clusterProfile, groupId: $"kafkasearch-pool-{Guid.NewGuid()}");

        return new RentedConsumer(this, new PooledConsumerEntry
        {
            Consumer = consumer,
            LastReturnedUtc = DateTime.UtcNow,
        });
    }

    internal void Return(PooledConsumerEntry entry, bool healthy)
    {
        if (!healthy)
        {
            entry.Consumer.Dispose();
            _creationGate.Release();
            return;
        }

        entry.LastReturnedUtc = DateTime.UtcNow;
        _idle.Push(entry);
    }

    public void Dispose()
    {
        _sweepTimer.Dispose();
        while (_idle.TryPop(out var entry))
            entry.Consumer.Dispose();
    }

    private void SweepIdle()
    {
        var cutoff = DateTime.UtcNow - _idleTimeout;
        var toKeep = new List<PooledConsumerEntry>();
        var aliveCount = _minSize;

        while (_idle.TryPop(out var entry))
        {
            var tooOld = entry.LastReturnedUtc < cutoff;
            var canAffordToEvict = _idle.Count + toKeep.Count >= _minSize;

            if (tooOld && canAffordToEvict)
            {
                entry.Consumer.Dispose();
                _creationGate.Release();
                _logger.LogInformation("Released idle consumer from pool for cluster {Cluster}", _clusterProfile.ClusterName);
            }
            else toKeep.Add(entry);
        }

        foreach (var entry in toKeep)
            _idle.Push(entry);
    }
}