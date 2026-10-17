namespace KafkaSearch.Core.Services;

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Services.Interfaces;
using TopicMetadata = Models.TopicMetadata;
using static KafkaSearch.Core.Services.TopicConfigKeys;

public class TopicService : ITopicService
{
    private readonly IConsumerGroupService _consumerGroupService;
    private readonly IClusterClientProvider _clusterClientProvider;
    private readonly IKafkaConsumerPoolManager _kafkaConsumerPoolManager;

    public TopicService(
        IClusterClientProvider clusterClientProvider,
        IKafkaConsumerPoolManager kafkaConsumerPoolManager,
        IConsumerGroupService consumerGroupService)
    {
        _clusterClientProvider = clusterClientProvider;
        _kafkaConsumerPoolManager = kafkaConsumerPoolManager;
        _consumerGroupService = consumerGroupService;
    }

    public OperationResult<TopicSummary[]> GetTopicsNames(string clusterProfileName)
    {
        var clientMetadataResult = _clusterClientProvider.MetadataFor(clusterProfileName);

        if (clientMetadataResult.IsFailure)
            return OperationResult.Fail(clientMetadataResult.Failure);

        var topicNames = clientMetadataResult.Value.Topics.Select(t => new TopicSummary(t.Topic, t.Partitions.Count)).ToArray();

        return OperationResult.Ok(topicNames);
    }

    public async Task<OperationResult<TopicMetadata>> DescribeTopic(
        string clusterProfileName,
        string topic,
        CancellationToken cancellationToken = default)
    {
        var metadataResult = _clusterClientProvider.MetadataFor(clusterProfileName);
        if (metadataResult.IsFailure)
            return OperationResult.Fail<TopicMetadata>(metadataResult.Failure);

        var topicMetadata = metadataResult.Value!.Topics.FirstOrDefault(t => t.Topic == topic);
        if (topicMetadata is null)
            return OperationResult.Fail<TopicMetadata>(
                Failure.Validation($"Topic '{topic}' not found on cluster '{clusterProfileName}'.", 404));

        var rentResult = await _kafkaConsumerPoolManager.RentAsync(clusterProfileName, cancellationToken);
        if (rentResult.IsFailure)
            return OperationResult.Fail<TopicMetadata>(rentResult.Failure);

        using var rented = rentResult.Value!;

        var partitions = new List<PartitionInfo>();
        long totalMessages = 0;

        try
        {
            foreach (var p in topicMetadata.Partitions)
            {
                var wm = rented.Consumer.QueryWatermarkOffsets(
                    new TopicPartition(topic, p.PartitionId), TimeSpan.FromSeconds(5));

                totalMessages += wm.High.Value - wm.Low.Value;
                partitions.Add(new PartitionInfo(
                    p.PartitionId, p.Leader, p.Replicas, p.InSyncReplicas, wm.Low.Value, wm.High.Value));
            }
        }
        catch (KafkaException)
        {
            rented.MarkUnhealthy(); // don't let a consumer that just errored go back in the idle stack as if nothing happened
            throw;
        }

        var highWatermarks = partitions.ToDictionary(p => p.PartitionId, p => p.HighWatermark);

        var lagTask = _consumerGroupService.GetLagForTopic(
            clusterProfileName,
            topic,
            highWatermarks,
            cancellationToken);

        var configTask = GetTopicConfigAsync(
            clusterProfileName,
            topic,
            cancellationToken);

        await Task.WhenAll(lagTask, configTask);

        var lagResult = await lagTask;
        var configResult = await configTask;

        if (lagResult.IsFailure)
            return OperationResult.Fail<TopicMetadata>(lagResult.Failure);

        if (configResult.IsFailure)
            return OperationResult.Fail<TopicMetadata>(configResult.Failure);

        return OperationResult.Ok(new TopicMetadata(
            topic,
            partitions.Count,
            partitions[0].Replicas.Length,
            totalMessages,
            configResult.Value!,
            partitions,
            lagResult.Value!));
    }

    private async Task<OperationResult<TopicConfig>> GetTopicConfigAsync(
        string clusterProfileName,
        string topic,
        CancellationToken cancellationToken)
    {
        var adminResult = _clusterClientProvider.ForCluster(clusterProfileName);
        if (adminResult.IsFailure)
            return OperationResult.Fail<TopicConfig>(adminResult.Failure);

        var resource = new ConfigResource { Name = topic, Type = ResourceType.Topic };
        var results = await adminResult.Value!.DescribeConfigsAsync(new[] { resource });
        var entries = results[0].Entries;

        return OperationResult.Ok(new TopicConfig(
            CleanupPolicy: entries.GetValueOrDefault("cleanup.policy")?.Value ?? "delete",
            RetentionMs: ParseInt(entries, RetentionMs),
            RetentionBytes: ParseLong(entries, RetentionBytes),
            MinInSyncReplicas: ParseInt(entries, MinInsyncReplicas),
            DeleteRetentionMs: ParseLong(entries, DeleteRetentionMs),
            CompressionType: entries.GetValueOrDefault(TopicConfigKeys.CompressionType)?.Value ?? string.Empty,
            FileDeleteDelayMs: ParseLong(entries, FileDeleteDelayMs),
            FlushMessages: ParseLong(entries, FlushMessages),
            FlushMs: ParseLong(entries, FlushMs),
            FollowerReplicationThrottledReplicas: GetString(entries, FollowerReplicationThrottledReplicas),
            IndexIntervalBytes: ParseLong(entries, IndexIntervalBytes),
            LeaderReplicationThrottledReplicas: GetString(entries, LeaderReplicationThrottledReplicas),
            MaxCompactionLagMs: ParseLong(entries, MaxCompactionLagMs),
            MaxMessageBytes: ParseLong(entries, MaxMessageBytes),
            MessageDownconversionEnable: ParseBool(entries, MessageDownconversionEnable),
            MessageFormatVersion: GetString(entries, MessageFormatVersion),
            MessageTimestampDifferenceMaxMs: ParseLong(entries, MessageTimestampDifferenceMaxMs),
            MessageTimestampType: GetString(entries, MessageTimestampType),
            MinCleanableDirtyRatio: ParseLong(entries, MinCleanableDirtyRatio),
            MinCompactionLagMs: ParseLong(entries, MinCompactionLagMs),
            Preallocate: ParseBool(entries, Preallocate),
            SegmentBytes: ParseLong(entries, SegmentBytes),
            SegmentIndexBytes: ParseLong(entries, SegmentIndexBytes),
            SegmentJitterMs: ParseLong(entries, SegmentJitterMs),
            SegmentMs: ParseLong(entries, SegmentMs),
            UncleanLeaderElectionEnable: ParseBool(entries, UncleanLeaderElectionEnable)));
    }

    private static long? ParseLong(Dictionary<string, ConfigEntryResult> entries, string key)
        => long.TryParse(entries.GetValueOrDefault(key)?.Value, out var value) ? value : null;

    private static int? ParseInt(Dictionary<string, ConfigEntryResult> entries, string key)
        => int.TryParse(entries.GetValueOrDefault(key)?.Value, out var value) ? value : null;

    private static bool? ParseBool(Dictionary<string, ConfigEntryResult> entries, string key)
        => bool.TryParse(entries.GetValueOrDefault(key)?.Value, out var value) ? value : null;

    private static double? ParseDouble(Dictionary<string, ConfigEntryResult> entries, string key)
        => double.TryParse(entries.GetValueOrDefault(key)?.Value, out var value) ? value : null;

    private static string GetString(Dictionary<string, ConfigEntryResult> entries, string key)
        => entries.GetValueOrDefault(key)?.Value ?? string.Empty;
}

internal static class TopicConfigKeys
{
    public const string CleanupPolicy = "cleanup.policy";
    public const string CompressionType = "compression.type";
    public const string DeleteRetentionMs = "delete.retention.ms";
    public const string FileDeleteDelayMs = "file.delete.delay.ms";
    public const string FlushMessages = "flush.messages";
    public const string FlushMs = "flush.ms";
    public const string FollowerReplicationThrottledReplicas = "follower.replication.throttled.replicas";
    public const string IndexIntervalBytes = "index.interval.bytes";
    public const string LeaderReplicationThrottledReplicas = "leader.replication.throttled.replicas";
    public const string MaxCompactionLagMs = "max.compaction.lag.ms";
    public const string MaxMessageBytes = "max.message.bytes";
    public const string MessageDownconversionEnable = "message.downconversion.enable";
    public const string MessageFormatVersion = "message.format.version";
    public const string MessageTimestampDifferenceMaxMs = "message.timestamp.difference.max.ms";
    public const string MessageTimestampType = "message.timestamp.type";
    public const string MinCleanableDirtyRatio = "min.cleanable.dirty.ratio";
    public const string MinCompactionLagMs = "min.compaction.lag.ms";
    public const string MinInsyncReplicas = "min.insync.replicas";
    public const string Preallocate = "preallocate";
    public const string RetentionBytes = "retention.bytes";
    public const string RetentionMs = "retention.ms";
    public const string SegmentBytes = "segment.bytes";
    public const string SegmentIndexBytes = "segment.index.bytes";
    public const string SegmentJitterMs = "segment.jitter.ms";
    public const string SegmentMs = "segment.ms";
    public const string UncleanLeaderElectionEnable = "unclean.leader.election.enable";
}
