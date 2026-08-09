using Confluent.Kafka;
using KafkaSearch.Core.Common;

namespace KafkaSearch.Core.Services.Interfaces;

public interface IClusterClientProvider
{
    OperationResult<IAdminClient> ForCluster(string clusterProfileName);
    OperationResult<Metadata> MetadataFor(string clusterName);
}