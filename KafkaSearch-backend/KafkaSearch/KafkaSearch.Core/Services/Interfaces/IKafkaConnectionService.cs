namespace KafkaSearch.Core.Services.Interfaces;

using Confluent.Kafka;
using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;

public interface IKafkaConnectionService
{
    OperationResult<IAdminClient> GetOrCreateAdminClient(ClusterProfile clusterProfile);
    void InvalidateConnection(string clusterName);
}
