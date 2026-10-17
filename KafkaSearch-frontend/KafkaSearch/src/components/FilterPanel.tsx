import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Paper,
  Typography
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import { useStyles } from "./css/FilterPanel.styles";
import { useState } from "react";
import FilterBuilder from "./FilterBuilder";

export const FilterPanel = () => {
  const [open, setOpen] = useState(true);

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
          <FilterBuilder />
        </AccordionDetails>
      </Accordion>
    </Paper>
  );
};
