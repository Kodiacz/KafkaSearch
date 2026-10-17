import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Paper,
  Typography
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import { useStyles } from "./css/FilterPanel.styles";
import { useEffect, useState } from "react";
import FilterBuilder from "./FilterBuilder";
import type { KafkaMessage } from "../types/kafka";
import { discoverFields, type DiscoveredField } from "../filtering/discoverFields";

type FilterPanelProps = {
  messages: KafkaMessage[];
};

export const FilterPanel = ({ messages }: FilterPanelProps) => {
  const [open, setOpen] = useState(true);
  const [discoveredFields, setDiscoveredFields] = useState<DiscoveredField[]>()

  useEffect(() => {
    const result = discoverFields(messages.map(x => x.value));
    console.log('result => ', result)
    setDiscoveredFields(result)
  }, [messages])
  

  console.log('discoveredFields => ', discoveredFields)

  const styles = useStyles();
  return (
    <Paper elevation={3} className={styles.panel}>
      <Accordion expanded={open} onChange={(_, v) => setOpen(v)}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          {/* <Typography>Filters {count > 0 && `(${count})`}</Typography>
    {!open && <FilterChips filter={filter} />} */}
          <Typography>Test Summary</Typography>
        </AccordionSummary>
        <AccordionDetails>
          {/* <FilterBuilder value={filter} onChange={setFilter} fields={fields} /> */}
          <FilterBuilder discovoredFields={discoveredFields ?? []}/>
        </AccordionDetails>
      </Accordion>
    </Paper>
  );
};
