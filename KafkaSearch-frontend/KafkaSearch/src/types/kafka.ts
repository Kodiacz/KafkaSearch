export interface KafkaMessage {
  topic: string;
  partition: number;
  offset: number;
  key: string | null;
  value: string | null;
  timestamp: number;
  header?: Record<string, string>;
}

export interface SearchMessagesParams {
  topic: string;
  filter?: string;
}

export type CompareOp =
  | "Equal"
  | "NotEqual"
  | "GreaterThan"
  | "GreaterThanOrEqual"
  | "LessThan"
  | "LessThanOrEqual"
  | "Contains"
  | "StartsWith"
  | "EndsWith"
  | "Exists";

export type FilterNode =
  | { type: "and"; nodes: FilterNode[] }
  | { type: "or"; nodes: FilterNode[] }
  | { type: "any"; path: string; where: FilterNode }
  | { type: "compare"; path: string; op: CompareOp; value: unknown };

export type keyFormat = "Auto" | "String" | "Int32" | "Int64" | "Hex";

export type seekMode = "Oldest" | "Newest" | "Offset" | "Timestamp";

export type ScanRequest = {
  clusterProfileName: string;
  topic: string;
  filter?: FilterNode | null;
  partition?: number | null;
  key?: string | null;
  keyFormat?: keyFormat | null;
  seekMode?: seekMode | null;
  offsetValue?: number | null;
  fromTimestamp?: string | null;
  maxMessagesPerPartition?: number;
};

export interface PartitionInfo {
  partitionId: number;
  leader: number;
  replicas: number[];
  inSyncReplicas: number[];
  lowWatermark: number;
  highWatermark: number;
}

export interface ConsumerGroupLag {
  groupId: string;
  state: string;
  totalLag: number;
  lagByPartition: Record<string, number>; // Kafka's int partition keys always serialize as string JSON object keys
}

export interface TopicConfig {
  cleanupPolicy: string;
  retentionMs: number | null;
  retentionBytes: number | null;
  minInSyncReplicas: number | null;
  deleteRetentionMs: number | null;
  compressionType: string;
  fileDeleteDelayMs: number | null;
  flushMessages: number | null;
  flushMs: number | null;
  followerReplicationThrottledReplicas: string;
  indexIntervalBytes: number | null;
  leaderReplicationThrottledReplicas: string;
  maxCompactionLagMs: number | null;
  maxMessageBytes: number | null;
  messageDownconversionEnable: boolean | null;
  messageFormatVersion: string;
  messageTimestampDifferenceMaxMs: number | null;
  messageTimestampType: string;
  minCleanableDirtyRatio: number | null;
  minCompactionLagMs: number | null;
  preallocate: boolean | null;
  segmentBytes: number | null;
  segmentIndexBytes: number | null;
  segmentJitterMs: number | null;
  segmentMs: number | null;
  uncleanLeaderElectionEnable: boolean | null;
}

export interface TopicMetadata {
  topic: string;
  partitionCount: number;
  replicationFactor: number;
  totalMessageCount: number;
  config: TopicConfig;
  partitions: PartitionInfo[];
  consumerGroups: ConsumerGroupLag[];
}
