namespace KafkaSearch.Core.Services;

using Confluent.Kafka;
using KafkaSearch.Core.Common;
using KafkaSearch.Core.Services.Interfaces;

public class ClusterClientProvider : IClusterClientProvider
{
    private IKafkaConnectionService _kafkaConnectionService;
    private IClusterProfileService _clusterProfileService;
    private TimeSpan _timeout = TimeSpan.FromSeconds(5);

    public ClusterClientProvider(
        IKafkaConnectionService kafkaConnectionService,
        IClusterProfileService clusterProfileService)
    {
        _kafkaConnectionService = kafkaConnectionService;
        _clusterProfileService = clusterProfileService;
    }

    public OperationResult<IAdminClient> ForCluster(string clusterProfileName)
    {
        var profile = _clusterProfileService.GetByName(clusterProfileName);

        if (profile.IsFailure)
            return OperationResult.Fail<IAdminClient>(profile.Failure);

        return _kafkaConnectionService.GetOrCreateAdminClient(profile.Value!);
    }

    public OperationResult<Metadata> MetadataFor(string clusterProfileName)
    {
        var clientResult = ForCluster(clusterProfileName);

        if (clientResult.IsFailure)
            return OperationResult.Fail<Metadata>(clientResult.Failure);

        return OperationResult.Try(() => clientResult.Value!.GetMetadata(_timeout));
    }
}
