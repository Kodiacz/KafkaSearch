namespace KafkaSearch.Core.Services;

using Confluent.Kafka;
using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Filtering;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Services.Interfaces;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

public class MessageScanService : IMessageScanService
{
    private IClusterClientProvider _clusterClientProvider;
    private IClusterProfileService _clusterProfileService;
    private IKafkaClientFactory _kafkaClientFactory;
    private IFilterEvaluator _filterEvaluator;
    private ILogger<MessageScanService> _logger;
    private TimeSpan _pollTimeout = TimeSpan.FromSeconds(1);

    public MessageScanService(
        IClusterClientProvider clusterClientProvider,
        IClusterProfileService clusterProfileService,
        IKafkaClientFactory kafkaClientFactory,
        IFilterEvaluator filterEvaluator,
        ILogger<MessageScanService> logger)
    {
        _clusterClientProvider = clusterClientProvider;
        _clusterProfileService = clusterProfileService;
        _kafkaClientFactory = kafkaClientFactory;
        _filterEvaluator = filterEvaluator;
        _logger = logger;
    }

    public async IAsyncEnumerable<KafkaMessage> Scan(
        string clusterProfileName,
        string topic,
        FilterNode filter,
        int maxMessagesPerPartition = 50_000,
        int? inputPartition = null,
        Offset? offset = null,
        DateTime? fromTimestamp = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var profileResult = _clusterProfileService.GetByName(clusterProfileName);
        if (profileResult.IsFailure)
            throw new InvalidOperationException(profileResult.Failure.Message);

        var metadataResult = _clusterClientProvider.MetadataFor(clusterProfileName);
        if (metadataResult.IsFailure)
            throw new InvalidOperationException(metadataResult.Failure.Message);

        var topicMetadata = metadataResult.Value!.Topics.FirstOrDefault(t => t.Topic == topic);
        if (topicMetadata is null)
            throw new InvalidOperationException($"Topic '{topic}' not found on cluster '{clusterProfileName}'.");

        using var consumer = _kafkaClientFactory.CreateConsumer(
            profileResult.Value!,
            groupId: $"kafkasearch-scan-{Guid.NewGuid()}");

        List<TopicPartitionOffset> assignments;

        var relevantPartitions = inputPartition.HasValue
            ? topicMetadata.Partitions.Where(p => p.PartitionId == inputPartition)
            : topicMetadata.Partitions;

        if (fromTimestamp.HasValue)
        {
            var timestampToSearch = relevantPartitions
                .Select(p => new TopicPartitionTimestamp(
                    topic,
                    p.PartitionId,
                    new Timestamp(fromTimestamp.Value.ToUniversalTime())));

            assignments = consumer.OffsetsForTimes(timestampToSearch, TimeSpan.FromSeconds(10));
        }
        else
        {
            var effectiveOffset = offset ?? Offset.Beginning;

            assignments = relevantPartitions
                .Select(p => new TopicPartitionOffset(topic, p.PartitionId, effectiveOffset))
                .ToList();
        }

        consumer.Assign(assignments);

        var pendingPartitions = assignments.Select(a => a.Partition.Value).ToHashSet();
        var readCounts = assignments.ToDictionary(a => a.Partition.Value, a => 0);

        var totalSw = Stopwatch.StartNew();
        long consumeRawTicks = 0;
        long evalRawTicks = 0;
        int messagesRead = 0;

        try
        {
            while (pendingPartitions.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var beforeConsumer = Stopwatch.GetTimestamp();

                var result = consumer.Consume(_pollTimeout);

                if (result is null)
                    continue;

                if (result.IsPartitionEOF)
                {
                    pendingPartitions.Remove(result.Partition.Value);
                    continue;
                }

                var partition = result.Partition.Value;

                readCounts[partition]++;

                var beforeEvaluate = Stopwatch.GetTimestamp();

                var message = TryEvaluate(result, topic, filter);

                evalRawTicks += Stopwatch.GetTimestamp() - beforeEvaluate;

                if (message is not null)
                {
                    yield return message;
                }
                else
                {
                    _logger.LogError($"No match for message: partition {partition} readCounts: {readCounts[partition]}");
                }

                if (readCounts[partition] >= maxMessagesPerPartition)
                {
                    pendingPartitions.Remove(partition);
                    consumer.Pause(new[] { result.TopicPartition });
                }
            }
        }
        finally
        {
            consumer.Close();

            var consumeMs = consumeRawTicks * 1000.0 / Stopwatch.Frequency;
            var evalMs = evalRawTicks * 1000.0 / Stopwatch.Frequency;

            _logger.LogInformation(
                "Scan stats: {Messages} messages, {TotalMs:F0}ms total, {ConsumeMs:F0}ms waiting on Kafka ({ConsumePct:F0}%), {EvalMs:F0}ms parsing+filtering ({EvalPct:F0}%)",
                messagesRead, totalSw.Elapsed.TotalMilliseconds,
                consumeMs, 100.0 * consumeMs / totalSw.Elapsed.TotalMilliseconds,
                evalMs, 100.0 * evalMs / totalSw.Elapsed.TotalMilliseconds);
        }
    }

    private KafkaMessage? TryEvaluate(ConsumeResult<byte[], byte[]> result, string topic, FilterNode filter)
    {
        string jsonResultValue;
        Dictionary<string, byte[]> jsonResultHeaders = new();

        try
        {
            using var docResultValue = JsonDocument.Parse(result.Message.Value);

            foreach (var header in result.Headers)
                jsonResultHeaders.TryAdd(header.Key, header.GetValueBytes());

            if (!_filterEvaluator.Evaluate(filter, docResultValue.RootElement))
                return null;

            return new KafkaMessage(
            topic,
            result.Partition.Value,
            result.Offset.Value,
            result.Message.Key is not null ? Encoding.UTF8.GetString(result.Message.Key) : null,
            docResultValue.RootElement.Clone(),
            jsonResultHeaders,
            result.Message.Timestamp.UtcDateTime);
        }
        catch (JsonException ex)
        {
            // malformed payload — skip this message, keep the scan alive
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to evaluate message at {Topic}[{Partition}]@{Offset}",
                topic, result.Partition.Value, result.Offset.Value);
            return null;
        }
    }
}