namespace KafkaSearch.Core.Services.Interfaces;

using KafkaSearch.Core.Filtering;
using KafkaSearch.Core.Models;

public interface IMessageScanService
{
    IAsyncEnumerable<KafkaMessage> Scan(
        string clusterProfileName,
        string topic,
        FilterNode filter,
        string key,
        KeyFormat keyFormat = KeyFormat.Auto,
        int maxMessagesPerPartition = 50_000,
        int? partition = null,
        SeekMode seekMode = SeekMode.Newest,
        long? offsetValue = null,
        DateTime? fromTimestamp = null,
        CancellationToken cancellationToken = default);
}