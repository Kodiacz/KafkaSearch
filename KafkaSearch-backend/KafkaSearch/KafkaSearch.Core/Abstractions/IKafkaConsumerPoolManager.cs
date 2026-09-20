namespace KafkaSearch.Core.Abstractions;

using KafkaSearch.Core.ConsumerPooling;

public interface IKafkaConsumerPoolManager
{
    Task<RentedConsumer> RentAsync(string clusterProfileName, CancellationToken ct = default);
}
