import { useState } from "react";
import Paper from "@mui/material/Paper";
import styles from "./css/MessageSearchPanel.module.css";
import SelectField from "./buttons/SelectField";
import NumberField from "./buttons/NumberField";
import TextField from "@mui/material/TextField";
import { DateTimePicker } from "@mui/x-date-pickers/DateTimePicker";
import { type Dayjs } from "dayjs";

const partitions = ["All", "Partition 1", "Partition 2", "Partition 3"];
const offsets = ["Newest", "Oldest", "Offset", "Timestamp"];

const MessageSearchPanel = () => {
  const [maxResults, setMaxResults] = useState(10);
  const [timestamp, setTimestamp] = useState<Dayjs | null>(null);

  const handleMaxResultsChange = (value: number) => {
    setMaxResults(value);
    console.log("Max Results changed to:", value);
  };

  return (
    <Paper elevation={3} className={styles.panel}>
      <SelectField labelName="Partitions" items={partitions} />
      <SelectField labelName="Offset" items={offsets} />
      <NumberField
        label="Max Results"
        min={1}
        max={50_000}
        onChange={handleMaxResultsChange}
        value={maxResults}
      />
      <TextField
        label="Quick Search"
        variant="outlined"
        placeholder="Search messages..."
      />
      <DateTimePicker label="From" value={timestamp} onChange={setTimestamp} />
    </Paper>
  );
};

export default MessageSearchPanel;
