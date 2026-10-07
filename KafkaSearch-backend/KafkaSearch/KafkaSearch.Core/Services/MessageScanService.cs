namespace KafkaSearch.Core.Services;

using Confluent.Kafka;
using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Filtering;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Services.Interfaces;
using Microsoft.Extensions.Logging;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
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
    private static readonly UTF8Encoding StrictUtf8 =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

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
        string key,
        KeyFormat keyFormat = KeyFormat.Auto,
        int maxMessagesPerPartition = 50_000,
        int? selectedPartition = null,
        SeekMode seekMode = SeekMode.Newest,
        long? offsetValue = null,
        DateTime? fromTimestamp = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var profileResult = _clusterProfileService.GetByName(clusterProfileName);
        if (profileResult.IsFailure)
            throw new InvalidOperationException(profileResult.Failure.Message);

        var metadataResult = _clusterClientProvider.MetadataFor(clusterProfileName);
        if (metadataResult.IsFailure)
            throw new InvalidOperationException(metadataResult.Failure.Message);

        if (seekMode == SeekMode.Offset && !offsetValue.HasValue)
            throw new ArgumentException("SeekMode.Offset requires an offsetValue to be provided.");

        var topicMetadata = metadataResult.Value!.Topics.FirstOrDefault(t => t.Topic == topic);
        if (topicMetadata is null)
            throw new InvalidOperationException($"Topic '{topic}' not found on cluster '{clusterProfileName}'.");

        using var consumer = _kafkaClientFactory.CreateConsumer(
            profileResult.Value!,
            groupId: $"kafkasearch-scan-{Guid.NewGuid()}");

        List<TopicPartitionOffset> assignments;

        var relevantPartitions = selectedPartition.HasValue
            ? topicMetadata.Partitions.Where(p => p.PartitionId == selectedPartition)
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
            assignments = seekMode switch
            {
                SeekMode.Newest => relevantPartitions.Select(p =>
                {
                    var wm = consumer.QueryWatermarkOffsets(
                        new TopicPartition(topic, p.PartitionId), TimeSpan.FromSeconds(5));
                    var start = Math.Max(wm.Low.Value, wm.High.Value - maxMessagesPerPartition);
                    return new TopicPartitionOffset(topic, p.PartitionId, new Offset(start));
                }).ToList(),

                SeekMode.Offset when offsetValue.HasValue => relevantPartitions
                    .Select(p => new TopicPartitionOffset(topic, p.PartitionId, new Offset(offsetValue.Value)))
                    .ToList(),

                _ => relevantPartitions
                    .Select(p => new TopicPartitionOffset(topic, p.PartitionId, Offset.Beginning))
                    .ToList(),
            };
        }

        consumer.Assign(assignments);

        var pendingPartitions = assignments.Select(a => a.Partition.Value).ToHashSet();
        var readCounts = assignments.ToDictionary(a => a.Partition.Value, a => 0);

        var totalSw = Stopwatch.StartNew();
        long consumeRawTicks = 0;
        long evalRawTicks = 0;
        int messagesRead = 0;
        int messagesMatched = 0;

        byte[]? expectedKey = string.IsNullOrEmpty(key) ? null : EncodeKey(key, keyFormat);

        try
        {
            while (pendingPartitions.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var beforeConsumer = Stopwatch.GetTimestamp();

                var result = consumer.Consume(_pollTimeout);

                consumeRawTicks += Stopwatch.GetTimestamp() - beforeConsumer;

                if (result is null)
                    continue;

                if (result.IsPartitionEOF)
                {
                    pendingPartitions.Remove(result.Partition.Value);
                    continue;
                }

                var partition = result.Partition.Value;

                messagesRead++;

                var beforeEvaluate = Stopwatch.GetTimestamp();

                var message = TryEvaluate(
                    result,
                    topic,
                    filter,
                    expectedKey,
                    keyFormat);

                readCounts[partition]++;

                evalRawTicks += Stopwatch.GetTimestamp() - beforeEvaluate;

                if (message is not null)
                {
                    messagesMatched++;
                    yield return message;
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

    private KafkaMessage? TryEvaluate(
        ConsumeResult<byte[], byte[]> result,
        string topic,
        FilterNode filter,
        byte[]? expectedKey,
        KeyFormat keyFormat)
    {
        string jsonResultValue;
        Dictionary<string, byte[]> jsonResultHeaders = new();

        var test = DecodeKey(result.Message.Key, KeyFormat.Int64);

        //if (test == "20419671") Debugger.Break();

        try
        {
            if (expectedKey is not null &&
            !result.Message.Key.AsSpan().SequenceEqual(expectedKey))
                return null;

            using var docResultValue = JsonDocument.Parse(result.Message.Value);

            foreach (var header in result.Message.Headers)
                jsonResultHeaders.TryAdd(header.Key, header.GetValueBytes());

            if (!_filterEvaluator.Evaluate(filter, docResultValue.RootElement))
                return null;

            return new KafkaMessage(
            topic,
            result.Partition.Value,
            result.Offset.Value,
            DecodeKey(result.Message.Key, keyFormat),
            docResultValue.RootElement.Clone(),
            jsonResultHeaders,
            result.Message.Timestamp.UtcDateTime);
        }
        catch (JsonException ex)
        {
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

    private static string? DecodeKey(byte[]? keyBytes, KeyFormat format) => format switch
    {
        _ when keyBytes is null => null,
        KeyFormat.Int32 when keyBytes.Length == 4 => BinaryPrimitives.ReadInt32BigEndian(keyBytes).ToString(),
        KeyFormat.Int64 when keyBytes.Length == 8 => BinaryPrimitives.ReadInt64BigEndian(keyBytes).ToString(),
        KeyFormat.Hex => Convert.ToHexString(keyBytes),
        _ => DecodeKeyAuto(keyBytes),
    };

    private static string? DecodeKeyAuto(byte[]? keyBytes)
    {
        if (keyBytes is null) return null;
        try { return StrictUtf8.GetString(keyBytes); }
        catch (DecoderFallbackException) { return Convert.ToHexString(keyBytes); }
    }

    private static byte[] EncodeKey(string key, KeyFormat format)
    {
        switch (format)
        {
            case KeyFormat.Int32:
                if (!int.TryParse(key, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i32))
                    throw new ArgumentException($"'{key}' is not a valid Int32 key.");
                var b32 = new byte[4];
                BinaryPrimitives.WriteInt32BigEndian(b32, i32);
                return b32;

            case KeyFormat.Int64:
                if (!long.TryParse(key, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i64))
                    throw new ArgumentException($"'{key}' is not a valid Int64 key.");
                var b64 = new byte[8];
                BinaryPrimitives.WriteInt64BigEndian(b64, i64);
                return b64;

            case KeyFormat.Hex:
                return Convert.FromHexString(key); // throws FormatException on bad input

            default: // Auto and String: for matching, Auto means "treat it as text"
                return Encoding.UTF8.GetBytes(key);
        }
    }
}