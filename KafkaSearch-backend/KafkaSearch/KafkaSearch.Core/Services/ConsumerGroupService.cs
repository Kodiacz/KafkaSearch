using Confluent.Kafka;
using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Services.Interfaces;

namespace KafkaSearch.Core.Services;

public class ConsumerGroupService : IConsumerGroupService
{
    private readonly IClusterClientProvider _clusterClientProvider;

    public ConsumerGroupService(IClusterClientProvider clusterClientProvider)
        => _clusterClientProvider = clusterClientProvider;

    public async Task<OperationResult<IReadOnlyList<ConsumerGroupLag>>> GetLagForTopic(
        string clusterProfileName, string topic,
        IReadOnlyDictionary<int, long> highWatermarksByPartition,
        CancellationToken cancellationToken = default)
    {
        var adminResult = _clusterClientProvider.ForCluster(clusterProfileName);
        if (adminResult.IsFailure)
            return OperationResult.Fail<IReadOnlyList<ConsumerGroupLag>>(adminResult.Failure);

        var adminClient = adminResult.Value!;
        var allGroups = await adminClient.ListConsumerGroupsAsync();

        if (allGroups.Valid.Count == 0)
            return OperationResult.Ok<IReadOnlyList<ConsumerGroupLag>>(Array.Empty<ConsumerGroupLag>());

        // null → broker returns every partition this group has committed offsets for, across ALL
        // topics; we filter down to just this topic below. Worth confirming against your installed
        // package version — some releases expect an empty List<TopicPartition>() instead of null.
        var offsetTasks = allGroups.Valid
            .Select(g =>
                adminClient.ListConsumerGroupOffsetsAsync(
                    new List<ConsumerGroupTopicPartitions> { new(g.GroupId, null) }));

        var offsetsPerGroupResult = await Task.WhenAll(offsetTasks);
        var offsetsPerGroup = offsetsPerGroupResult.SelectMany(r => r).ToList();
        var result = new List<ConsumerGroupLag>();

        foreach (var groupOffsets in offsetsPerGroup)
        {
            var relevant = groupOffsets.Partitions
                .Where(p => p.Topic == topic && p.Offset.Value >= 0)
                .ToList();

            if (relevant.Count == 0)
                continue; // this group doesn't touch this topic at all

            var lagByPartition = relevant.ToDictionary(
                p => p.Partition.Value,
                p => Math.Max(0, highWatermarksByPartition.GetValueOrDefault(p.Partition.Value) - p.Offset.Value));

            var state = allGroups.Valid.First(g => g.GroupId == groupOffsets.Group).State.ToString();

            result.Add(new ConsumerGroupLag(groupOffsets.Group, state, lagByPartition.Values.Sum(), lagByPartition));
        }

        return OperationResult.Ok<IReadOnlyList<ConsumerGroupLag>>(result);
    }
}
