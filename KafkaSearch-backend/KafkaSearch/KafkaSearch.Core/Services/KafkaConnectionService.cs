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
        IClusterProfileService clusterProfileService, 
        IKafkaClientFactory kafkaClientFactory) 
    { 
        _kafkaClientFactory = kafkaClientFactory;
    }

    public OperationResult<IAdminClient> GetOrCreateAdminClient(ClusterProfile clusterProfile)
    {
        if (clusterProfile == null)
            return OperationResult.Fail(new ArgumentNullException(nameof(clusterProfile), "Cluster profile cannot be null"));

        if (_adminClientCache.TryGetValue(clusterProfile.ClusterName, out var existingClient))
            return OperationResult.Ok(existingClient);

        var result = OperationResult.Try<IAdminClient>(() =>
        {
            var client = _kafkaClientFactory.Create(clusterProfile);
            var verified = Verify(client);

            if (verified.IsFailure)
            {
                client.Dispose();
                return null;
            }

            _adminClientCache.TryAdd(clusterProfile.ClusterName, client);
            return client;
        });

        if (result.IsFailure || result.Value is null)
            return OperationResult.Fail(Failure.Operation($"Failed to create or verify admin client for cluster '{clusterProfile.ClusterName}'"));

        return result;
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

    private OperationResult Verify(IAdminClient client)
    => OperationResult.Try(() => client.GetMetadata(_metadataTimeout));
}