import { Box, IconButton, TextField } from "@mui/material";
import DeleteIcon from "@mui/icons-material/Delete";
import SelectField from "./buttons/SelectField";
import type { ConditionData } from "./FilterBuilder";
import { COMPARE_OPS } from "../types/kafka";

type ConditionProps = {
  conditionBoxStyle: string;
  value: ConditionData;
  onChange: (patch: Partial<ConditionData>) => void;
  onRemove: () => void;
};

const Condition = ({
  conditionBoxStyle,
  value,
  onChange,
  onRemove
}: ConditionProps) => (
  <Box className={conditionBoxStyle}>
    <SelectField
      labelName="Json field"
      items={["test 1", "test 2"]}
      value={value.field}
      onChange={(field) => onChange({ field })}
    />
    <SelectField
      labelName="Operations"
      items={COMPARE_OPS}
      value={value.op}
      onChange={(op) => onChange({ op })}
    />
    <TextField
      label="Value"
      value={value.value}
      onChange={(e) => onChange({ value: e.target.value })}
    />
    <IconButton aria-label="Remove condition" onClick={onRemove}>
      <DeleteIcon />
    </IconButton>
  </Box>
);

export default Condition;
