import * as React from "react";
import Box from "@mui/material/Box";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import FormControl from "@mui/material/FormControl";
import Select, { type SelectChangeEvent } from "@mui/material/Select";
import styles from "../css/SelectField.module.css";

type SelectFieldProps = {
  labelName: string;
  items: string[];
};

export default function SelectField({ labelName, items }: SelectFieldProps) {
  const [selectedItem, setSelectedItem] = React.useState(items[0]);

  const handleChange = (event: SelectChangeEvent) => {
    setSelectedItem(event.target.value as string);
  };

  return (
    <Box className={styles.selectFieldBox}>
      <FormControl fullWidth>
        <InputLabel>{labelName}</InputLabel>
        <Select
          value={selectedItem}
          label={labelName}
          onChange={handleChange}
        >
          {items.map((item, index) => (
            <MenuItem key={index} value={item}>
              {item}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
    </Box>
  );
}
