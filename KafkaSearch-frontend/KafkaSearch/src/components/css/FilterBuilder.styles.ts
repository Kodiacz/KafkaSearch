import { createUseStyles } from "react-jss";

export const useStyles = createUseStyles({
  rootBox: { display: "flex", flexDirection: "column", gap: '20px' },
  combinatorBox: { display: "flex", flexDirection: "row" },
  conditionBox: { display: "flex", flexDirection: "row", gap: '16px'},
});
