namespace KafkaSearch.Core.Services.Interfaces;

using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;

public interface IKafkaConnectionService
{
    OperationResult<AdminClientWithMetadata> GetOrCreateAdminClient(ClusterProfile clusterProfile);
    void InvalidateConnection(string clusterName);
}
