import Paper from "@mui/material/Paper";
import Box from "@mui/material/Box";

const MessageSearchPanel = () => {
  return (
    <Box
        className="MessageSearchPanel"
      sx={{Zindex: 1, position: "relative", width: "100px", height: "100px", borderColor: "black", borderStyle: "solid", borderWidth: 1, margin: 2}}>
      <Paper
        elevation={24}
        sx={{ height: 600, lineHeight: "600px", textAlign: "center" }}
      >
        {/* <p style={{ textAlign: "center", margin: 0 }}>Message Search Panel</p> */}
        Test
      </Paper>
    </Box>
  );
};

export default MessageSearchPanel;
