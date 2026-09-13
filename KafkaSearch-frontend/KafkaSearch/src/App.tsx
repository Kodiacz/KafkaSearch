import './App.css'
import MessageSearchPanel from './components/MessageSearchPanel'
import Messages from './pages/Messages'
import Box from "@mui/material/Box";

function App() {

  return (
    <Box sx={{ display: "flex", flexDirection: "row", width: "100%", height: "100%" }}>

      <Messages/>
      <MessageSearchPanel></MessageSearchPanel>
    </Box>
  )
}

export default App
