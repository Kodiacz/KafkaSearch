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

const partitions = ["All", "Partition 1", "Partition 2", "Partition 3"];
const offsets = ["Newest", "Oldest", "Offset", "Timestamp"];

const MessageSearchPanel = ({topic, partitions}: {topic: string, partitions: string[]}) => {
  const [maxResults, setMaxResults] = useState(10);
  const [timestamp, setTimestamp] = useState<Dayjs | null>(null);
  const [searchMode, setSearchMode] = useState<string>("Newest");

  const styles = useStyles();

  const handleMaxResultsChange = (value: number) => {
    setMaxResults(value);
    console.log("Max Results changed to:", value);
  };

  const handleFetch = () => {
    scanMessages(
      {
        clusterProfileName: "dev",
        topic: topic,
        filter: {
          type: "and",
          nodes: []
        },
        partition: null,
        key: "",
        keyFormat: "Auto",
        seekMode: "Newest",
        offsetValue: null,
        fromTimestamp: null,
        maxMessagesPerPartition: 5
      },
      () => console.log("Message received")
    );

    console.log("Fetch button clicked with parameters:");
  };

  console.log("Current search mode:", searchMode);

  return (
    <Paper elevation={3} className={styles.panel}>
      <SelectField labelName="Partitions" items={partitions} />
      <SelectField
        labelName="Offset"
        items={offsets}
        onChange={(value) => setSearchMode(value)}
      />
      <NumberField
        label="Max Results"
        min={1}
        max={50_000}
        onChange={handleMaxResultsChange}
        value={maxResults}
      />
      <TextField label="Key" variant="outlined" placeholder="Key" />
      <TextField
        label="Quick Search"
        variant="outlined"
        placeholder="Search messages..."
      />
      {searchMode === "Timestamp" && (
        <DateTimePicker
          label="From"
          value={timestamp}
          onChange={setTimestamp}
        />
      )}
      <Button variant="contained" onClick={handleFetch}>
        Fetch
      </Button>
    </Paper>
  );
};

export default MessageSearchPanel;
