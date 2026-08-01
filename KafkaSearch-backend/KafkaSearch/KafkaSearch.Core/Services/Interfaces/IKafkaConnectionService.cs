namespace KafkaSearch.Core.Services.Interfaces;

using Confluent.Kafka;
using KafkaSearch.Core.Common;

public interface IKafkaConnectionService
{
    OperationResult<IAdminClient> GetOrCreateAdminClient(string clusterName);
    void InvalidateConnection(string clusterName);
}
