namespace KafkaSearch.Core.Services.Interfaces;

using Confluent.Kafka;
using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;

public interface IKafkaConnectionService
{
    OperationResult CreateAdminClient(ClusterProfile clusterProfile);
    OperationResult<IAdminClient> GetOrCreateAdminClient(string clusterName);
    void InvalidateConnection(string clusterName);
}
