namespace KafkaSearch.Core.Models;

using Confluent.Kafka;

public sealed record AdminClientWithMetadata(IAdminClient Client, Metadata? FreshMetadata);
