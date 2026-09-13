export interface KafkaMessage {
  topic: string,
  partition: number,
  offset: number,
  key: string | null,
  value: string | null,
  timestamp: number,
  header?: Record<string, string>,
}

export interface SearchMessagesParams {
    topic: string,
    filter?: string,
}