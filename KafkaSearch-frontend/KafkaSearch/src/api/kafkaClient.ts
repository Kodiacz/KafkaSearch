import type {
  KafkaMessage,
  ScanRequest,
  SearchMessagesParams,
  TopicMetadata
} from "../types/kafka";

const BASE_URL = import.meta.env.VITE_API_BASE_URL;

export async function getTopicNames(
  clusterProfileName: string,
  signal: AbortSignal
): Promise<string[]> {
  const url = `https://localhost:7208/api/topics/names?clusterProfile=${encodeURIComponent(clusterProfileName)}`;
  const response = await fetch(url, { signal });

  if (!response.ok) {
    throw new Error(
      `Failed to load topics: ${response.status} ${response.statusText}`
    );
  }

  return response.json();
}

export async function describeTopic(
  clusterProfileName: string,
  topic: string,
  signal?: AbortSignal
): Promise<TopicMetadata> {
  const url = `https://localhost:7208/api/topics/describe/${topic}?clusterProfile=${encodeURIComponent(clusterProfileName)}`;
  const response = await fetch(url, { signal });
  if (!response.ok)
    throw new Error(
      `Failed to describe topic: ${response.status} ${response.statusText}`
    );
  return response.json();
}

export async function scanMessages(
  request: ScanRequest,
  onMessage: (message: KafkaMessage) => void,
  signal?: AbortSignal
): Promise<void> {
  const url = `https://localhost:7208/api/search/scan`;
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
    signal
  });

  if (!response.ok) {
    throw new Error(`Scan failed: ${response.status} ${response.statusText}`);
  }

  // Deferred, per your plan — real SSE frame parsing (event:/data: splitting)
  // goes here later. For now this just proves the connection opens and data
  // is actually arriving, so you can verify the round trip before writing
  // the parser.
  const reader = response.body?.getReader();
  const decoder = new TextDecoder();
  if (!reader) return;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    console.log("raw chunk:", decoder.decode(value, { stream: true }));
  }
}
