namespace KafkaSearch.Core.Services;

using Confluent.Kafka;
using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Services.Interfaces;
using System.Collections.Concurrent;

public class KafkaConnectionService : IKafkaConnectionService, IDisposable
{
    private ConcurrentDictionary<string, IAdminClient> _adminClientCache = new();
    private IClusterProfileService _clusterProfileService;

    public KafkaConnectionService(IClusterProfileService clusterProfileService)
    {
        _clusterProfileService = clusterProfileService;
    }

    public OperationResult CreateAdminClient(ClusterProfile clusterProfile)
    {
        if (clusterProfile == null) 
            return OperationResult.Fail(new ArgumentNullException(nameof(clusterProfile), "Cluster profile cannot be null"));

        return OperationResult.Try(() =>
        {
            var client = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = clusterProfile.BootstrapServers
            }).Build();

            _adminClientCache.TryAdd(clusterProfile.ClusterName, client);
        });
    }

    public OperationResult<IAdminClient> GetOrCreateAdminClient(string clusterName)
    {
        if (_adminClientCache.TryGetValue(clusterName, out var existingClient))
            return OperationResult.Ok(existingClient);

        var profileResult = _clusterProfileService.GetByName(clusterName);

        if (profileResult.IsFailure)
            return OperationResult.Fail<IAdminClient>(profileResult.Failure);

        return OperationResult.Try<IAdminClient>(() =>
        {
            var client = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = profileResult.Value!.BootstrapServers
            }).Build();

            _adminClientCache.TryAdd(profileResult.Value.ClusterName, client);
            return client;
        });
    }

    public void InvalidateConnection(string clusterName)
    {
        throw new NotImplementedException();
    }

    public void Dispose()
    {
        foreach (var client in _adminClientCache.Values)
        {
            client.Dispose();
        }
    }
}