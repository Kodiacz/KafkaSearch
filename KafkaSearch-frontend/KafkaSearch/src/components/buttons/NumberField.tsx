import TextField from "@mui/material/TextField";
import styles from "../css/SelectField.module.css";

interface NumberFieldProps {
  label: string;
  value: number;
  onChange: (value: number) => void;
  min?: number;
  max?: number;
}

const NumberField = ({ label, value, onChange, min, max }: NumberFieldProps) => (
  <TextField
    className={styles.maxResultField}
    type="number"
    label={label}
    value={value}
    onChange={(e) => onChange(Number(e.target.value))}
    slotProps={{ htmlInput: { min, max } }}
  />
);

export default NumberField;