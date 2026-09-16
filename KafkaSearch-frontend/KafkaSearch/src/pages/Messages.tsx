// src/pages/Messages.tsx
import { useState } from "react";
import Box from "@mui/material/Box";
import { searchMessages } from "../api/kafkaClient";
import type { KafkaMessage } from "../types/kafka";
import MessageSearchPanel from "../components/MessageSearchPanel";
import styles from "../components/css/MessageSearchPanel.module.css";

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
    <Box className={styles.panel}>
        <MessageSearchPanel/>
    </Box>
  );
}