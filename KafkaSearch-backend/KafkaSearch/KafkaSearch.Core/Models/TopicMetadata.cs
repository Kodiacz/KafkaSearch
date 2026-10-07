namespace KafkaSearch.Core.Models;

public sealed record TopicMetadata(
    string Topic,
    int PartitionCount,
    int ReplicationFactor,
    long TotalMessageCount,
    TopicConfig Config,
    IReadOnlyList<PartitionInfo> Partitions,
    IReadOnlyList<ConsumerGroupLag> ConsumerGroups);
