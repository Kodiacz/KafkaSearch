import "./App.css";
import MessageSearchPanel from "./components/MessageSearchPanel";
import Messages from "./pages/Messages";
import Box from "@mui/material/Box";

function App() {
  return (
    <Box sx={{ height: "100%", display: "flex", flexDirection: "column" }}>
      <MessageSearchPanel />
    </Box>
  );
}

export default App;
