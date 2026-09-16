import { createUseStyles } from "react-jss";

interface SelectFieldStyleProps {
  labelLength: number;
}

export const useStyles = createUseStyles({
  selectFieldBox: {
    minWidth: (props: SelectFieldStyleProps) => `${props.labelLength + 4}ch`,
  },
});
