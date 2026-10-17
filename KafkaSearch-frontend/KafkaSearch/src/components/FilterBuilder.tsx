import {
  Box,
  Button,
  TextField,
  ToggleButton,
  ToggleButtonGroup
} from "@mui/material";
import AddSharpIcon from "@mui/icons-material/AddSharp";
import { useState, type MouseEvent } from "react";
import SelectField from "./buttons/SelectField";
import { useStyles } from "./css/FilterBuilder.styles";
import Condition from "./Condition";

export type ConditionData = {
  id: string;
  field: string;
  op: string;
  value: string;
};

const FilterBuilder = () => {
  const [toggleValue, setToggleValue] = useState<string>("");
  const [conditions, setConditions] = useState<ConditionData[]>([]);

  const styles = useStyles();

  const handleToggleButtonChange = (
    _: MouseEvent<HTMLElement>,
    newToggleValue: string
  ) => {
    setToggleValue(newToggleValue);
  };

  const handleAddCondition = () => {
    setConditions((prev) => [
      ...prev,
      { id: crypto.randomUUID(), field: "", op: "Equal", value: "" }
    ]);
  };

  const handleUpdateCondition = (id: string, patch: Partial<ConditionData>) => {
  setConditions((prev) => prev.map((c) => (c.id === id ? { ...c, ...patch } : c)));
};

const handleRemoveCondition = (id: string) => {
  setConditions((prev) => prev.filter((c) => c.id !== id));
};
  console.log("toggleValue", toggleValue);

  return (
    <Box className={styles.rootBox}>
      <Box className={styles.combinatorBox}>
        <ToggleButtonGroup
          exclusive
          color="primary"
          aria-label="Platform"
          onChange={handleToggleButtonChange}
          value={toggleValue}
        >
          <ToggleButton value={"AND"}>AND</ToggleButton>
          <ToggleButton value={"OR"}>OR</ToggleButton>
        </ToggleButtonGroup>
        <Button endIcon={<AddSharpIcon />} onClick={handleAddCondition}>Add condition</Button>
      </Box>
      {conditions.map((c) => (
  <Condition
    key={c.id}
    conditionBoxStyle={styles.conditionBox}
    value={c}
    onChange={(patch) => handleUpdateCondition(c.id, patch)}
    onRemove={() => handleRemoveCondition(c.id)}
  />
))}
    </Box>
  );
};

export default FilterBuilder;
