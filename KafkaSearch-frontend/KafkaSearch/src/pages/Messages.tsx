// src/pages/Messages.tsx
import { useState } from "react";
import { searchMessages } from "../api/kafkaClient";
import type { KafkaMessage } from "../types/kafka";

export default function Messages() {
  const [topic, setTopic] = useState("");
  const [filter, setFilter] = useState("");
  const [results, setResults] = useState<KafkaMessage[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleSearch() {
    setLoading(true);
    setError(null);
    try {
      setResults(await searchMessages({ topic, filter }));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Something went wrong");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      <input value={topic} onChange={(e) => setTopic(e.target.value)} placeholder="Topic name" />
      <input value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="Filter" />
      <button onClick={handleSearch} disabled={loading || !topic}>
        {loading ? "Searching..." : "Search"}
      </button>

      {error && <p style={{ color: "red" }}>{error}</p>}
      {results && <pre>{JSON.stringify(results, null, 2)}</pre>}
    </div>
  );
}