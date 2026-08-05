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
        if (clusterProfile is null)
            return OperationResult.Fail<IAdminClient>(Failure.Validation("Cluster profile cannot be null."));

        if (_adminClientCache.TryGetValue(clusterProfile.ClusterName, out var existingClient))
            return OperationResult.Ok(existingClient);

        var buildResult = OperationResult.Try(() => _kafkaClientFactory.Create(clusterProfile));

        if (buildResult.IsFailure)
            return OperationResult.Fail<IAdminClient>(buildResult.Failure);

        var client = buildResult.Value!;
        var verified = Verify(client);

        if (verified.IsFailure)
        {
            client.Dispose();
            return OperationResult.Fail<IAdminClient>(verified.Failure);
        }

        return OperationResult.Ok(_adminClientCache.GetOrAdd(clusterProfile.ClusterName, client));
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

    public OperationResult<Metadata> GetAdminClientMetadata(string clusterName)
    {
        if (!_adminClientCache.TryGetValue(clusterName, out var client))
            return OperationResult.Fail<Metadata>(
                Failure.Operation($"Admin client for cluster '{clusterName}' does not exist.", 404));

        return OperationResult.Try(() => client.GetMetadata(_metadataTimeout));
    }

    private OperationResult Verify(IAdminClient client)
    => OperationResult.Try(() => client.GetMetadata(_metadataTimeout));
}