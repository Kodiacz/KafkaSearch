import "./App.css";
import { LocalizationProvider } from "@mui/x-date-pickers";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import CssBaseline from "@mui/material/CssBaseline";
import Messages from "./pages/Messages";

function App() {
  return (
    <LocalizationProvider dateAdapter={AdapterDayjs}>
      <CssBaseline />
        <Messages />
    </LocalizationProvider>
  );
}

export default App;
