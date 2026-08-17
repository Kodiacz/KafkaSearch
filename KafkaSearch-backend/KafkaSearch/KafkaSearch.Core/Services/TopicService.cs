namespace KafkaSearch.Core.Services;

using KafkaSearch.Core.Common;
using KafkaSearch.Core.Services.Interfaces;

public class TopicService : ITopicService
{
    private IClusterClientProvider _kafkaConnectionService;

    public TopicService(IClusterClientProvider kafkaConnectionService)
    {
        _kafkaConnectionService = kafkaConnectionService;
    }

    public OperationResult<string[]> GetTopicsNames(string clusterProfileName)
    {
        var clientMetadataResult = _kafkaConnectionService.MetadataFor(clusterProfileName);

        if (clientMetadataResult.IsFailure)
            return OperationResult.Fail(clientMetadataResult.Failure);

        var topicNames = clientMetadataResult.Value.Topics.Select(t => t.Topic).ToArray();
        
        return OperationResult.Ok(topicNames);
    }
}
