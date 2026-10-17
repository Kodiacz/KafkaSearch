import { useMemo, useState } from "react";
import Box from "@mui/material/Box";
import type {
  KafkaMessage,
  PartitionInfo,
  TopicMetadata,
  TopicSummary
} from "../types/kafka";
import MessageSearchPanel from "../components/MessageSearchPanel";
import styles from "./css/Messages.module.css";
import ResultPanel from "../components/ResultPanel.";
import TopicSelectorPanel from "../components/TopicSelectorPanel";
import { useMessageScan } from "../hooks/useMessageScan";
import useLazyAsync from "../hooks/useLazyAsync";
import { describeTopic, getTopicNames as getTopics } from "../api/kafkaClient";
import useAsync from "../hooks/useAsync";
import { FilterPanel } from "../components/FilterPanel";

export default function Messages() {
  const [topicName, setTopic] = useState<string | undefined>(undefined);
  const scan = useMessageScan();

  const handleTopicChange = (topic: string | null) => {
    setTopic(topic ?? "");
  };

  const { data: topics, loading: loadingTopics } = useAsync<TopicSummary[]>(
    (signal) => getTopics("dev", signal),
    []
  );

  const partitionCount =
    topics?.find((t) => t.name === topicName)?.partitionCount ?? 0;

  const partitionIds = useMemo(
    () => Array.from({ length: partitionCount }, (_, i) => i),
    [partitionCount]
  );

  console.log('topicName', topicName)
  console.log('partitionCount', partitionCount)

  return (
    <Box className={styles.messageBox}>
      <TopicSelectorPanel
        onTopicChange={handleTopicChange}
        selectedTopic={topicName}
        loadingTopics={loadingTopics}
        topics={topics?.map((t) => t.name) ?? []}
      />
      <FilterPanel />
      <MessageSearchPanel
        topic={topicName ?? ""}
        partitionIds={partitionIds}
        onFetch={scan.start}
        onCancel={scan.cancel}
      />
      <ResultPanel />
    </Box>
  );
}
