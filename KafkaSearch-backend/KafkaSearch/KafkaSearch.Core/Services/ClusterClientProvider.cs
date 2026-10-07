namespace KafkaSearch.Core.Services;

using Confluent.Kafka;
using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;
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
        var result = ResolveClient(clusterProfileName);
        if (result.IsFailure)
            return OperationResult.Fail<IAdminClient>(result.Failure);

        return OperationResult.Ok(result.Value!.Client);
    }

    public OperationResult<Metadata> MetadataFor(string clusterProfileName)
    {
        var result = ResolveClient(clusterProfileName);
        if (result.IsFailure)
            return OperationResult.Fail<Metadata>(result.Failure);

        var resolved = result.Value!;

        if (resolved.FreshMetadata is not null)
            return OperationResult.Ok(resolved.FreshMetadata);

        return OperationResult.Try(() => resolved.Client.GetMetadata(_timeout));
    }

    private OperationResult<AdminClientWithMetadata> ResolveClient(string clusterProfileName)
    {
        var profile = _clusterProfileService.GetByName(clusterProfileName);

        if (profile.IsFailure)
            return OperationResult.Fail<AdminClientWithMetadata>(profile.Failure);

        return _kafkaConnectionService.GetOrCreateAdminClient(profile.Value!);
    }
}
