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

export const COMPARE_OPS = [
  "Equal",
  "NotEqual",
  "GreaterThan",
  "GreaterThanOrEqual",
  "LessThan",
  "LessThanOrEqual",
  "Contains",
  "StartsWith",
  "EndsWith",
  "Exists",
] as const;

export type CompareOp = (typeof COMPARE_OPS)[number];

export type FilterNode =
  | { type: "and"; nodes: FilterNode[] }
  | { type: "or"; nodes: FilterNode[] }
  | { type: "any"; path: string; where: FilterNode }
  | { type: "compare"; path: string; op: CompareOp; value: unknown };

export type KeyFormat = "Auto" | "String" | "Int32" | "Int64" | "Hex";

export const SEEK_MODES = ["Oldest", "Newest", "Offset", "Timestamp"] as const;

export const SeekMode = {
  Newest: "Newest",
  Oldest: "Oldest",
  Offset: "Offset",
  Timestamp: "Timestamp",
} as const;

export type SeekMode = (typeof SeekMode)[keyof typeof SeekMode];

export type ScanRequest = {
  clusterProfileName: string;
  topic: string;
  filter?: FilterNode | null;
  partition?: number | null;
  key?: string | null;
  keyFormat?: KeyFormat | null;
  seekMode?: SeekMode | null;
  offsetValue?: number | null;
  fromTimestamp?: string | null;
  maxMessagesPerPartition?: number;
};

export interface PartitionInfo {
  highWatermark: number;
  inSyncReplicas: number[];
  leader: number;
  lowWatermark: number;
  partitionId: number;
  replicas: number[];
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

export type TopicSummary = {
  name: string;
  partitionCount: number;
}