const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
dotenv.config();

const app = express();
const PORT = process.env.PORT || 5002;
const HOST = process.env.HOST || "0.0.0.0"; 

const mainRoutes = require("./routes/masterDataRoutes");

const allowedOrigins = [
  "http://localhost:5173",
   process.env.FRONTEND_URL || "http://13.53.61.186:5002"
  ];

app.use(cors({
  origin: function (origin, callback) {
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error("Not allowed by CORS"));
    }
  }
}));

app.use(express.json());

app.use("/api", mainRoutes);

app.get("/", (req, res) => {
  res.send("Backend API is running");
});

app.listen(PORT, HOST, () => {
  console.log(`Server is running on http://${HOST}:${PORT}`);
});