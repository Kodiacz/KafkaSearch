namespace KafkaSearch.Core.Abstractions;

using Confluent.Kafka;
using KafkaSearch.Core.Models;

public interface IKafkaClientFactory
{
    IAdminClient Create(ClusterProfile profile);
    IConsumer<byte[], byte[]> CreateConsumer(ClusterProfile profile, string groupId);
}
