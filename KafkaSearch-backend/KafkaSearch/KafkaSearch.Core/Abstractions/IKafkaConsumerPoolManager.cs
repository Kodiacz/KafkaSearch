namespace KafkaSearch.Core.Abstractions;

using KafkaSearch.Core.Common;
using KafkaSearch.Core.ConsumerPooling;

public interface IKafkaConsumerPoolManager
{
    Task<OperationResult<RentedConsumer>> RentAsync(string clusterProfileName, CancellationToken ct = default);
}
