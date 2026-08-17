namespace KafkaSearch.Core.Services;

using Confluent.Kafka;
using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Filtering;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Services.Interfaces;
using Microsoft.Extensions.Logging;
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

        var assignments = topicMetadata.Partitions
            .Select(p => new TopicPartitionOffset(topic, p.PartitionId, Offset.Beginning))
            .ToList();

        consumer.Assign(assignments);

        var pendingPartitions = assignments.Select(a => a.Partition.Value).ToHashSet();
        var readCounts = assignments.ToDictionary(a => a.Partition.Value, a => 0);

        try
        {
            while (pendingPartitions.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();

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

                var message = TryEvaluate(result, topic, filter);

                if (message is not null)
                    yield return message;

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