namespace KafkaSearch.Core.ConsumerPooling;

using System.Collections.Concurrent;
using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Services.Interfaces;

public sealed class KafkaConsumerPoolManager : IKafkaConsumerPoolManager, IDisposable
{
    private readonly IClusterProfileService _clusterProfileService;
    private readonly IKafkaClientFactory _kafkaClientFactory;
    private readonly ConcurrentDictionary<string, ClusterConsumerPool> _pools
        = new();

    public KafkaConsumerPoolManager(
        IClusterProfileService clusterProfileService,
        IKafkaClientFactory kafkaClientFactory)
    {
        _clusterProfileService = clusterProfileService;
        _kafkaClientFactory = kafkaClientFactory;
    }

    public async Task<RentedConsumer> RentAsync(
        string clusterProfileName,
        CancellationToken ct = default)
    {
        var pool = _pools.GetOrAdd(clusterProfileName, name =>
        {
            var profile = _clusterProfileService.GetByName(name).Value!;
            return new ClusterConsumerPool(_kafkaClientFactory, profile);
        });

        return await pool.RentAsync(ct);
    }

    public void Dispose()
    {
        foreach (var pool in _pools.Values)
            pool.Dispose();
    }
}
