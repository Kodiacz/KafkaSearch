namespace KafkaSearch.Core.Services.Interfaces;

using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;

public interface IConsumerGroupService
{
    public Task<OperationResult<IReadOnlyList<ConsumerGroupLag>>> GetLagForTopic(
        string clusterProfileName, string topic,
        IReadOnlyDictionary<int, long> highWatermarksByPartition,
        CancellationToken cancellationToken = default);
}