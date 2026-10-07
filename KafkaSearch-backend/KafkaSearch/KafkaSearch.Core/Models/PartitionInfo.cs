namespace KafkaSearch.Core.Models;

public sealed record PartitionInfo(
    int PartitionId, 
    int Leader, 
    int[] Replicas, 
    int[] InSyncReplicas,
    long LowWatermark, 
    long HighWatermark);
