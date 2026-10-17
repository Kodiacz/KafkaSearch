import * as React from "react";
import Box from "@mui/material/Box";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import FormControl from "@mui/material/FormControl";
import Select, { type SelectChangeEvent } from "@mui/material/Select";
import { useStyles } from "../css/SelectField.styles.ts";

type SelectFieldProps<T extends string> = {
  labelName: string;
  items: readonly T[];
  value?: T;
  onChange?: (value: T) => void;
};

export default function SelectField<T extends string>({
  labelName,
  items,
  value,
  onChange
}: SelectFieldProps<T>) {
  const [internalValue, setInternalValue] = React.useState(items[0]);

  const isControlled = value !== undefined;
  const selectedItem = isControlled ? value : internalValue;

  const classes = useStyles();

  const handleChange = (event: SelectChangeEvent) => {
    const next = event.target.value as T; // one cast, kept inside the component
    if (!isControlled) setInternalValue(next);
    onChange?.(next);
  };

  return (
    <Box className={classes.formControl}>
      <FormControl>
        <InputLabel>{labelName}</InputLabel>
        <Select
          value={selectedItem}
          label={labelName}
          onChange={handleChange}
          sx={{ minWidth: 120 }}
        >
          {items.map((item) => (
            <MenuItem key={item} value={item}>
              {item}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
    </Box>
  );
}
