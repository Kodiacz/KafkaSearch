import { createUseStyles } from "react-jss";

interface SelectFieldStyleProps {
  labelLength: number;
}

export const useStyles = createUseStyles({
  formControl: {
    minWidth: (props: SelectFieldStyleProps) => `${props.labelLength + 4}ch`,
  },
});