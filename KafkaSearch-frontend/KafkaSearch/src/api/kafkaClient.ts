import type {
  KafkaMessage,
  ScanRequest,
  SearchMessagesParams,
  TopicMetadata,
  TopicSummary
} from "../types/kafka";
import { ApiError, type ValidationProblem } from "./ApiError";

const BASE_URL = import.meta.env.VITE_API_BASE_URL;

export async function getTopicNames(
  clusterProfileName: string,
  signal: AbortSignal
): Promise<TopicSummary[]> {
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

  // kafkaClient.ts, in the failure path
  if (!response.ok) {
    const text = await response.text();
    let problem: ValidationProblem | null = null;
    try {
      problem = JSON.parse(text);
    } catch {
      /* not JSON */
    }
    throw new ApiError(response.status, problem, text);
  }

  // Deferred, per your plan — real SSE frame parsing (event:/data: splitting)
  // goes here later. For now this just proves the connection opens and data
  // is actually arriving, so you can verify the round trip before writing
  // the parser.
  const reader = response.body?.getReader();
  if (!reader) return;

  const decoder = new TextDecoder();
  let buffer = "";

  const dispatchFrame = (frame: string) => {
    // a frame is one or more lines; we only care about "data:" lines
    const data = frame
      .split(/\r?\n/)
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).replace(/^ /, ""))
      .join("\n");

    if (!data) return;
    onMessage(JSON.parse(data) as KafkaMessage);
  };

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });

    // frames are separated by a blank line
    const frames = buffer.split(/\r?\n\r?\n/);
    buffer = frames.pop() ?? ""; // last piece may be incomplete, keep it

    for (const frame of frames) dispatchFrame(frame);
    // console.log("raw chunk:", decoder.decode(value, { stream: true }));
  }

  buffer += decoder.decode();
  if (buffer.trim()) dispatchFrame(buffer);
}
