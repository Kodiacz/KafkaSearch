namespace KafkaSearch.Core.Services.Interfaces;

using KafkaSearch.Core.Common;

public interface ITopicService
{
    public OperationResult<string[]> GetTopicsNames(string clusterProfileName);
}
