import TextField from "@mui/material/TextField";
import { useStyles } from "../css/NumberField.styles.ts";


interface NumberFieldProps {
  label: string;
  value: number;
  onChange: (value: number) => void;
  min?: number;
  max?: number;
}

const NumberField = ({ label, value, onChange, min, max }: NumberFieldProps) => {

  const classes = useStyles();
  
  return (
    <TextField
    className={classes.selectFieldBox}
    type="number"
    label={label}
    value={value}
    onChange={(e) => onChange(Number(e.target.value))}
    slotProps={{ htmlInput: { min, max } }}
  />
);
}

export default NumberField;