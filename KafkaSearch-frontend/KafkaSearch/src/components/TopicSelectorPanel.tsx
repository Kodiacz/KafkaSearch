import {
  Autocomplete,
  Box,
  Button,
  Modal,
  Paper,
  TextField
} from "@mui/material";
import { useEffect, useState } from "react";
import { useStyles } from "./css/TopicSelectorPanel.styles.ts";
import { describeTopic, getTopicNames } from "../api/kafkaClient.ts";
import { matchSorter } from "match-sorter";
import useAsync from "../hooks/useAsync.tsx";
import useLazyAsync from "../hooks/useLazyAsync.tsx";
import type { PartitionInfo, TopicMetadata, TopicSummary } from "../types/kafka.ts";

type TopicSelectorPanelProps = {
  topics: string[];
  selectedTopic: string | undefined;
  loadingTopics: boolean;
  onTopicChange?: (topic: string | null) => void;
};

const TopicSelectorPanel = ({
  topics,
  selectedTopic,
  loadingTopics,
  onTopicChange,
}: TopicSelectorPanelProps) => {
  const styles = useStyles();
  const [openTopicDescriptionModal, setOpenTopicDescriptionModal] =
    useState<boolean>(false);

  const filterOptions = (
    options: string[],
    { inputValue }: { inputValue: string }
  ) => matchSorter(options, inputValue);

  const {
    run: fetchTopicDescription,
    data: topicMetadata,
    loading: loadingMetadata,
    error: metadataError
  } = useLazyAsync<TopicMetadata>();

  const handleDescribeTopic = () => {
    setOpenTopicDescriptionModal(true);

    if (selectedTopic)
      fetchTopicDescription((signal) => describeTopic("dev", selectedTopic, signal));
  };

  useEffect(() => {
    console.log(topicMetadata);
  }, [topicMetadata]);

  return (
    <Paper elevation={3} className={styles.panel}>
      <Autocomplete
        value={selectedTopic}
        onChange={(event, newValue) => {
          onTopicChange?.(newValue);
        }}
        disablePortal
        options={topics}
        filterOptions={filterOptions}
        sx={{ width: 300 }}
        renderInput={(params) => <TextField {...params} label="Topics" />}
        loading={loadingTopics}
      />
      <Button
        variant="contained"
        onClick={handleDescribeTopic}
        loading={loadingMetadata}
        disabled={!selectedTopic || loadingMetadata}
      >
        Describe Topic
      </Button>
      <Modal
        open={openTopicDescriptionModal}
        onClose={() => setOpenTopicDescriptionModal(false)}
      >
        <Box>
          {topicMetadata
            ? `Topic Description: ${topicMetadata.topic}`
            : "No description available."}
        </Box>
      </Modal>
    </Paper>
  );
};

export default TopicSelectorPanel;
