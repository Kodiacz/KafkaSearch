namespace KafkaSearch.Core.ConsumerPooling; 

using Confluent.Kafka;

internal sealed class PooledConsumerEntry
{
    public required IConsumer<byte[], byte[]> Consumer { get; init; }
    public DateTime LastReturnedUtc { get; set; }
}
