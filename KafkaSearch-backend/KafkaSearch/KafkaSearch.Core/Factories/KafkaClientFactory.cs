namespace KafkaSearch.Core.Factories;

using Confluent.Kafka;
using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Models;

public class KafkaClientFactory : IKafkaClientFactory
{
    public IAdminClient Create(ClusterProfile profile)
        => new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = profile.BootstrapServers
        }).Build();

    public IConsumer<byte[], byte[]> CreateConsumer(ClusterProfile profile, string groupId)
        => new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = profile.BootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true
        }).Build();
}
