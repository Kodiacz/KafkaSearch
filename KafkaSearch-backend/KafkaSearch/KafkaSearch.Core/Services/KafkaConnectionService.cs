namespace KafkaSearch.Core.Services;

using Confluent.Kafka;
using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Services.Interfaces;
using System.Collections.Concurrent;

public class KafkaConnectionService : IKafkaConnectionService, IDisposable
{
    private ConcurrentDictionary<string, IAdminClient> _adminClientCache = new();
    private IKafkaClientFactory _kafkaClientFactory;

    private TimeSpan _metadataTimeout = TimeSpan.FromSeconds(5);

    public KafkaConnectionService(
        IKafkaClientFactory kafkaClientFactory)
    {
        _kafkaClientFactory = kafkaClientFactory;
    }

    public OperationResult<AdminClientWithMetadata> GetOrCreateAdminClient(ClusterProfile clusterProfile)
    {
        if (clusterProfile is null)
            return OperationResult.Fail<AdminClientWithMetadata>(Failure.Validation("Cluster profile cannot be null."));

        if (_adminClientCache.TryGetValue(clusterProfile.ClusterName, out var existingClient))
            return OperationResult.Ok(new AdminClientWithMetadata(existingClient, FreshMetadata: null));

        var buildResult = OperationResult.Try(() => _kafkaClientFactory.Create(clusterProfile));

        if (buildResult.IsFailure)
            return OperationResult.Fail<AdminClientWithMetadata>(buildResult.Failure);

        var client = buildResult.Value!;
        var verified = GetVerifiedMetadata(client);

        if (verified.IsFailure)
        {
            client.Dispose();
            return OperationResult.Fail<AdminClientWithMetadata>(verified.Failure);
        }

        var cachedClient = _adminClientCache.GetOrAdd(clusterProfile.ClusterName, client);

        return OperationResult.Ok(new AdminClientWithMetadata(cachedClient, verified.Value));
    }

    public void InvalidateConnection(string clusterName)
    {
        if (_adminClientCache.TryRemove(clusterName, out var client))
            client.Dispose();
    }

    public void Dispose()
    {
        foreach (var client in _adminClientCache.Values)
            client.Dispose();
    }

    private OperationResult<Metadata> GetVerifiedMetadata(IAdminClient client)
        => OperationResult.Try(() => client.GetMetadata(_metadataTimeout));
}