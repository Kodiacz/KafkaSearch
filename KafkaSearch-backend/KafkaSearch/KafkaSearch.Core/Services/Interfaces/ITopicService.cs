namespace KafkaSearch.Core.Services.Interfaces;

using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;

public interface ITopicService
{
    Task<OperationResult<TopicMetadata>> DescribeTopic(string clusterProfileName, string topic, CancellationToken cancellationToken = default);
    OperationResult<TopicSummary[]> GetTopicsNames(string clusterProfileName);
}
