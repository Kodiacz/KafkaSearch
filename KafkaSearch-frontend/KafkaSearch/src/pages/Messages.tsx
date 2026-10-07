// src/pages/Messages.tsx
import { useState } from "react";
import Box from "@mui/material/Box";
import type { KafkaMessage } from "../types/kafka";
import MessageSearchPanel from "../components/MessageSearchPanel";
import styles from "./css/Messages.module.css";
import ResultPanel from "../components/ResultPanel.";
import TopicSelectorPanel from "../components/TopicSelectorPanel";

export default function Messages() {
  const [topic, setTopic] = useState("");
  const [filter, setFilter] = useState("");
  const [results, setResults] = useState<KafkaMessage[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  return (
    <Box className={styles.messageBox}>
      <TopicSelectorPanel/>
        <MessageSearchPanel/>
        <ResultPanel/>
    </Box>
  );
}