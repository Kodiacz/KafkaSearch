import { Box, IconButton, TextField } from "@mui/material";
import DeleteIcon from "@mui/icons-material/Delete";
import SelectField from "./buttons/SelectField";
import type { ConditionData } from "./FilterBuilder";
import { COMPARE_OPS } from "../types/kafka";
import type { DiscoveredField } from "../filtering/discoverFields";
import { useEffect, useState } from "react";

type ConditionProps = {
  conditionBoxStyle: string;
  discoveredFields: DiscoveredField[];
  value: ConditionData;
  onChange: (patch: Partial<ConditionData>) => void;
  onRemove: () => void;
};

const Condition = ({
  conditionBoxStyle,
  discoveredFields,
  value,
  onChange,
  onRemove
}: ConditionProps) => {
  const [selectedField, setSelectedField] = useState<DiscoveredField>()
  const [availableOps, setAvailableOps] = useState<string[]>([])

  useEffect(() => {
    const disocoveredField = discoveredFields.find(d => d.path === value.field)
    
    setSelectedField(disocoveredField)

    setAvailableOps(disocoveredField?.availableOps ?? [])
  }, [value.field])
  

  console.log('selectedField => ', selectedField)
  console.log('availableOps => ', availableOps)

  return (
    <Box className={conditionBoxStyle}>
      <SelectField
        labelName="Json field"
        items={discoveredFields.filter((x) => x.type != 'array-of-objects' || x.type != 'array-of-objects').map(x => x.path)}
        value={value.field}
        onChange={(field) => onChange({ field })}
      />
      <SelectField
        labelName="Operations"
        items={availableOps}
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
};
export default Condition;
