import { useState } from "react";
import Paper from "@mui/material/Paper";
import SelectField from "./buttons/SelectField";
import NumberField from "./buttons/NumberField";
import TextField from "@mui/material/TextField";
import { DateTimePicker } from "@mui/x-date-pickers/DateTimePicker";
import { type Dayjs } from "dayjs";
import { useStyles } from "./css/TopicSelectorPanel.styles.ts";
import { Button } from "@mui/material";
import { scanMessages } from "../api/kafkaClient.ts";
import { SEEK_MODES, SeekMode, type PartitionInfo } from "../types/kafka.ts";

type MessageSearchPanelProps = {
  topic: string;
  partitionIds: number[];
  onFetch: any;
  onCancel?: () => void;
};

const MessageSearchPanel = ({
  topic,
  partitionIds: partitionsIds,
  onFetch
}: MessageSearchPanelProps) => {
  const [maxResults, setMaxResults] = useState(10);
  const [timestamp, setTimestamp] = useState<Dayjs | null>(null);
  const [searchMode, setSearchMode] = useState<string>("Newest");
  const [partition, setPartition] = useState<string | null>(null);
  const [key, setKey] = useState<string | null>(null);
  const [offsetValue, setOffsetValue] = useState<number | null>(null);

  const styles = useStyles();

  const handleMaxResultsChange = (value: number) => {
    setMaxResults(value);
    console.log("Max Results changed to:", value);
  };

  const handleOffsetChange = (value: number) => {
    setOffsetValue(value);
    console.log("Max Results changed to:", value);
  };

  const handleFetch = () => {
    if (!topic) return;

    onFetch({
      clusterProfileName: "dev",
      topic,
      filter: { type: "and", nodes: [] },
      partition: partition === "All" ? null : Number(partition),
      key,
      keyFormat: "Auto",
      seekMode: searchMode,
      offsetValue: isSeekModeOffset ? offsetValue : null,
      fromTimestamp: isSeekModeTimestamp
        ? (timestamp?.toISOString() ?? null)
        : null,
      maxMessagesPerPartition: maxResults
    });
  };

  const isSeekModeTimestamp = searchMode === SeekMode.Timestamp;
  const isSeekModeOffset = searchMode === SeekMode.Offset;

  return (
    <Paper elevation={3} className={styles.panel}>
      <SelectField
        labelName="Partitions"
        items={partitionsIds.map((id) => id.toString())}
        onChange={(value) => setPartition(value)}
      />
      <SelectField
        labelName="Seek Mode"
        items={SEEK_MODES}
        onChange={(value) => setSearchMode(value)}
      />
      {isSeekModeTimestamp && (
        <DateTimePicker
          label="From"
          value={timestamp}
          onChange={setTimestamp}
        />
      )}
      {isSeekModeOffset && (
        <NumberField
          label="Offset Value"
          onChange={handleOffsetChange}
          value={maxResults}
        />
      )}
      <NumberField
        label="Max Results"
        min={1}
        max={50_000}
        onChange={handleMaxResultsChange}
        value={maxResults}
      />
      <TextField
        label="Key"
        variant="outlined"
        placeholder="Key"
        onChange={(e) => setKey(e.target.value)}
      />
      <TextField
        label="Quick Search"
        variant="outlined"
        placeholder="Search messages..."
      />
      <Button variant="contained" onClick={handleFetch}>
        Fetch
      </Button>
    </Paper>
  );
};

export default MessageSearchPanel;
