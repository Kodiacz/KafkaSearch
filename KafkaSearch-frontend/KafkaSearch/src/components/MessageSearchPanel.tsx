import Paper from "@mui/material/Paper";
import styles from "./css/MessageSearchPanel.module.css";
import MultipleSelect from "./buttons/MultipleSelect";

const MessageSearchPanel = () => {
  return (
    <Paper
      elevation={3}
      className={styles.panel}
    >
        <MultipleSelect/>
    </Paper>
  );
};

export default MessageSearchPanel;
