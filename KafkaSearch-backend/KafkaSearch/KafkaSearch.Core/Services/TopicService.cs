namespace KafkaSearch.Core.Services;

using KafkaSearch.Core.Common;
using KafkaSearch.Core.Services.Interfaces;

public class TopicService : ITopicService
{
    private IKafkaConnectionService _kafkaConnectionService;

    public TopicService(IKafkaConnectionService kafkaConnectionService)
    {
        _kafkaConnectionService = kafkaConnectionService;
    }

    public OperationResult<string[]> GetTopicsNames(string clusterProfileName)
    {
        var clientMetadataResult = _kafkaConnectionService.GetAdminClientMetadata(clusterProfileName);

        if (clientMetadataResult.IsFailure)
            return OperationResult.Fail(clientMetadataResult.Failure);

        var topicNames = clientMetadataResult.Value.Topics.Select(t => t.Topic).ToArray();
        
        return OperationResult.Ok(topicNames);
    }
}
