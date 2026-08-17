namespace KafkaSearch.Core.Services.Interfaces;

using KafkaSearch.Core.Filtering;
using KafkaSearch.Core.Models;

public interface IMessageScanService
{
    IAsyncEnumerable<KafkaMessage> Scan(
        string clusterProfileName,
        string topic,
        FilterNode filter,
        int maxMessagesPerPartition = 50_000,
        CancellationToken cancellationToken = default);
}