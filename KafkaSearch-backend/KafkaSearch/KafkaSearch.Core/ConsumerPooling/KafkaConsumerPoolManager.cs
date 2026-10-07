namespace KafkaSearch.Core.ConsumerPooling;

using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Common;
using KafkaSearch.Core.Services.Interfaces;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

public sealed class KafkaConsumerPoolManager : IKafkaConsumerPoolManager, IDisposable
{
    private readonly IClusterProfileService _clusterProfileService;
    private readonly IKafkaClientFactory _kafkaClientFactory;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ConcurrentDictionary<string, ClusterConsumerPool> _pools
        = new();

    public KafkaConsumerPoolManager(
        IClusterProfileService clusterProfileService,
        IKafkaClientFactory kafkaClientFactory,
        ILoggerFactory loggerFactory)
    {
        _clusterProfileService = clusterProfileService;
        _kafkaClientFactory = kafkaClientFactory;
        _loggerFactory = loggerFactory;
    }

    public async Task<OperationResult<RentedConsumer>> RentAsync(
        string clusterProfileName,
        CancellationToken ct = default)
    {
        var profileResult = _clusterProfileService.GetByName(clusterProfileName);
        if (profileResult.IsFailure) return OperationResult.Fail(profileResult.Failure);

        var pool = _pools.GetOrAdd(
            clusterProfileName,
            _ => new ClusterConsumerPool(
                _kafkaClientFactory, 
                profileResult.Value!, 
                _loggerFactory.CreateLogger<ClusterConsumerPool>()));

        var rented = await pool.RentAsync();
        return OperationResult.Ok(rented);
    }

    public void Dispose()
    {
        foreach (var pool in _pools.Values)
            pool.Dispose();
    }
}