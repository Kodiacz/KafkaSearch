namespace KafkaSearch.Core.Services;

using Confluent.Kafka;
using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Services.Interfaces;
using System.Collections.Concurrent;

public class KafkaConnectionService : IKafkaConnectionService, IDisposable
{
    private ConcurrentDictionary<string, IAdminClient> _adminClientCache = new();

    public KafkaConnectionService(IClusterProfileService clusterProfileService) { }

    public OperationResult<IAdminClient> GetOrCreateAdminClient(ClusterProfile clusterProfile)
    {
        if (clusterProfile == null)
            return OperationResult.Fail(new ArgumentNullException(nameof(clusterProfile), "Cluster profile cannot be null"));

        if (_adminClientCache.TryGetValue(clusterProfile.ClusterName, out var existingClient))
            return OperationResult.Ok(existingClient);

        return OperationResult.Try<IAdminClient>(() =>
        {
            var client = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = clusterProfile.BootstrapServers
            }).Build();

            _adminClientCache.TryAdd(clusterProfile.ClusterName, client);
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