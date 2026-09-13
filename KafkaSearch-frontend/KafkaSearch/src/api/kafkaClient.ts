import type { KafkaMessage, SearchMessagesParams } from "../types/kafka";

const BASE_URL = import.meta.env.VITE_API_BASE_URL;

export async function searchMessages(
  params: SearchMessagesParams
): Promise<KafkaMessage[]> {
  const query = new URLSearchParams({
    topic: params.topic,
    ...(params.filter ? { filter: params.filter } : {}),
  });

  const response = await fetch(`${BASE_URL}/messages/search?${query}`);

  if (!response.ok) {
    throw new Error(`Search failed: ${response.status} ${response.statusText}`);
  }

  return response.json();
}